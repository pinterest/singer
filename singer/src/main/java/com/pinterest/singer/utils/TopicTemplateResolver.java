/**
 * Copyright 2026 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.singer.utils;

import com.pinterest.singer.common.LogStream;
import com.pinterest.singer.common.SingerLog;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a Kafka topic template (writer.kafka.topicTemplate) for a Kubernetes pod
 * log stream.
 *
 * Supported variables:
 *   %{namespace}      - the Kubernetes namespace, parsed from the pod log directory name
 *                       which follows the kubelet convention namespace_podname[_uid]
 *   %{container}      - the first directory under the pod's log directory; with the
 *                       standard kubelet layout /var/log/pods/&lt;ns&gt;_&lt;pod&gt;_&lt;uid&gt;/&lt;container&gt;/
 *                       this is the container name
 *   %{metadata:key}   - a pod metadata value fetched via KubeConfig.podMetadataFields
 *                       (e.g. "labels:app" is looked up as %{metadata:app})
 *
 * Pod-level identifiers (pod name, pod uid) are intentionally unsupported: they change on
 * every pod restart / reschedule and would create unbounded Kafka topic cardinality.
 *
 * Resolution is all-or-nothing: if any variable cannot be resolved, or the resolved string
 * is not a legal Kafka topic name, an empty Optional is returned and the caller is expected
 * to fall back to the statically configured topic.
 */
public class TopicTemplateResolver {

  private static final Logger LOG = LoggerFactory.getLogger(TopicTemplateResolver.class);

  public static final String VAR_NAMESPACE = "namespace";
  public static final String VAR_CONTAINER = "container";
  public static final String VAR_METADATA = "metadata";

  private static final Pattern VARIABLE_PATTERN = Pattern.compile("%\\{([^}]*)\\}");
  // Kafka legal topic characters
  private static final Pattern LEGAL_TOPIC_PATTERN = Pattern.compile("[a-zA-Z0-9._-]+");
  private static final int MAX_TOPIC_LENGTH = 249;

  private TopicTemplateResolver() {
  }

  /**
   * Validate a topic template at configuration load time.
   *
   * @throws IllegalArgumentException if the template references an unsupported variable,
   *         contains malformed variable syntax, or has literal characters that are not
   *         legal in a Kafka topic name.
   */
  public static void validateTemplate(String template) {
    if (template == null || template.isEmpty()) {
      throw new IllegalArgumentException("topicTemplate must not be empty");
    }
    Matcher matcher = VARIABLE_PATTERN.matcher(template);
    StringBuffer literal = new StringBuffer();
    boolean hasVariable = false;
    while (matcher.find()) {
      hasVariable = true;
      String variable = matcher.group(1);
      String name = variable.contains(":")
          ? variable.substring(0, variable.indexOf(':')) : variable;
      String argument = variable.contains(":")
          ? variable.substring(variable.indexOf(':') + 1) : null;
      switch (name) {
        case VAR_NAMESPACE:
        case VAR_CONTAINER:
          if (argument != null) {
            throw new IllegalArgumentException("topicTemplate variable %{" + name
                + "} does not take an argument: %{" + variable + "}");
          }
          break;
        case VAR_METADATA:
          if (argument == null || argument.isEmpty()) {
            throw new IllegalArgumentException(
                "topicTemplate variable %{metadata:<key>} requires a key: %{" + variable + "}");
          }
          break;
        case "podName":
        case "podUid":
        case "pod":
          throw new IllegalArgumentException("topicTemplate variable %{" + name
              + "} is not supported: pod-level identifiers change on every pod restart and"
              + " would create unbounded topic cardinality. Allowed variables: %{namespace},"
              + " %{container}, %{metadata:<key>}");
        default:
          throw new IllegalArgumentException("Unsupported topicTemplate variable %{" + variable
              + "}. Allowed variables: %{namespace}, %{container}, %{metadata:<key>}");
      }
      matcher.appendReplacement(literal, "");
    }
    matcher.appendTail(literal);
    if (!hasVariable) {
      throw new IllegalArgumentException("topicTemplate \"" + template
          + "\" contains no %{...} variables; use writer.kafka.topic for static topics");
    }
    String literalPart = literal.toString();
    if (literalPart.contains("%{") || literalPart.contains("}")) {
      throw new IllegalArgumentException(
          "topicTemplate \"" + template + "\" has malformed %{...} variable syntax");
    }
    if (!literalPart.isEmpty() && !LEGAL_TOPIC_PATTERN.matcher(literalPart).matches()) {
      throw new IllegalArgumentException("topicTemplate \"" + template
          + "\" contains characters that are not legal in a Kafka topic name;"
          + " legal characters are [a-zA-Z0-9._-]");
    }
  }

  /**
   * Resolve a topic template for the given log stream.
   *
   * @param template        a template previously validated by {@link #validateTemplate}
   * @param logStream       the log stream the writer is being created for
   * @param podLogDirectory the configured Kubernetes pod log directory
   * @return the resolved topic, or empty if the stream is not a pod stream, any variable
   *         is unresolvable, or the result is not a legal Kafka topic name
   */
  public static Optional<String> resolve(String template, LogStream logStream,
                                         String podLogDirectory) {
    SingerLog singerLog = logStream.getSingerLog();
    String podUid = singerLog.getPodUid();
    if (podUid == null || podUid.isEmpty()) {
      LOG.debug("topicTemplate is only supported for Kubernetes pod log streams, stream {}"
          + " has no pod context", logStream.getLogStreamDescriptor());
      return Optional.empty();
    }
    Matcher matcher = VARIABLE_PATTERN.matcher(template);
    StringBuffer resolved = new StringBuffer();
    while (matcher.find()) {
      String variable = matcher.group(1);
      String value;
      if (VAR_NAMESPACE.equals(variable)) {
        value = extractNamespace(podUid);
      } else if (VAR_CONTAINER.equals(variable)) {
        value = extractContainerName(logStream.getLogDir(), podLogDirectory, podUid);
      } else if (variable.startsWith(VAR_METADATA + ":")) {
        String key = variable.substring(VAR_METADATA.length() + 1);
        value = singerLog.getMetadata(key).map(TopicTemplateResolver::decode).orElse(null);
      } else {
        // validateTemplate rejects these at config load; defensive for direct callers
        value = null;
      }
      if (value == null || value.isEmpty()) {
        LOG.warn("Could not resolve topicTemplate variable %{{{}}} for stream {}", variable,
            logStream.getLogStreamDescriptor());
        return Optional.empty();
      }
      matcher.appendReplacement(resolved, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(resolved);
    String topic = resolved.toString();
    if (!isLegalTopicName(topic)) {
      LOG.warn("Resolved topic \"{}\" for stream {} is not a legal Kafka topic name", topic,
          logStream.getLogStreamDescriptor());
      return Optional.empty();
    }
    return Optional.of(topic);
  }

  /**
   * Pod log directories follow the kubelet convention namespace_podname[_uid]; namespaces
   * are DNS labels and cannot contain underscores, so the namespace is the segment before
   * the first underscore.
   */
  public static String extractNamespace(String podUid) {
    int separatorIndex = podUid.indexOf('_');
    return separatorIndex > 0 ? podUid.substring(0, separatorIndex) : null;
  }

  /**
   * The container name is the first path segment of the stream's directory below
   * the pod's log directory, per the kubelet layout
   * /var/log/pods/&lt;ns&gt;_&lt;pod&gt;_&lt;uid&gt;/&lt;container&gt;/0.log
   */
  public static String extractContainerName(String streamLogDir, String podLogDirectory,
                                            String podUid) {
    if (streamLogDir == null || podLogDirectory == null || podLogDirectory.isEmpty()) {
      return null;
    }
    String podRoot = new File(new File(podLogDirectory), podUid).toPath().normalize().toString();
    String normalizedStreamDir = new File(streamLogDir).toPath().normalize().toString();
    if (!normalizedStreamDir.startsWith(podRoot + File.separator)) {
      return null;
    }
    String relative = normalizedStreamDir.substring(podRoot.length() + 1);
    int separatorIndex = relative.indexOf(File.separatorChar);
    return separatorIndex == -1 ? relative : relative.substring(0, separatorIndex);
  }

  public static boolean isLegalTopicName(String topic) {
    return !topic.isEmpty() && topic.length() <= MAX_TOPIC_LENGTH && !topic.equals(".")
        && !topic.equals("..") && LEGAL_TOPIC_PATTERN.matcher(topic).matches();
  }

  private static String decode(ByteBuffer buffer) {
    return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
  }
}
