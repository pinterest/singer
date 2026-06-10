# Running Singer on Kubernetes

This sample shows how to run Singer as a node-level logging agent (DaemonSet)
that tails logs for every pod on the node and routes each source to its own
Kafka topic.

## How Kubernetes mode works

With `singer.kubernetesEnabled=true`, Singer:

1. Watches `singer.kubernetes.podLogDirectory` (normally `/var/log/pods`) for
   pod directories, which follow the kubelet naming convention
   `<namespace>_<podname>_<poduid>`.
2. Polls the kubelet `/pods` API to detect pod deletions and to fetch pod
   metadata (labels, annotations, etc.) selected by
   `singer.kubernetes.podMetadataFields`.
3. For every pod, instantiates each log config in `conf.d/` whose `logDir`
   (interpreted *relative to the pod's log directory*) matches, creating
   independent log streams with their own watermarks.
4. Drains in-flight data before cleaning up when a pod is deleted.

## Tailing container stdout/stderr

The kubelet writes container output to
`/var/log/pods/<namespace>_<podname>_<poduid>/<container>/0.log` and rotates by
renaming (`0.log` → `0.log.<timestamp>`), which is compatible with Singer's
inode-based watermarks.

Use a wildcard `logDir` so every container directory becomes its own stream
(see `conf.d/container.stdout_logs.properties`):

```properties
logDir=/*
logStreamRegex=0.log
logFileMatchMode=prefix
```

Lines are shipped in the raw CRI format
(`<timestamp> <stdout|stderr> <P|F> <message>`); downstream consumers parse the
prefix if they need it.

## Per-source topic routing

`writer.kafka.topicTemplate` derives the topic from where the stream was
collected. Resolution happens once per stream, with this precedence:

1. **`topicTemplate`** — used when every variable resolves and the result is a
   legal Kafka topic name (`[a-zA-Z0-9._-]`, max 249 chars). Variables:
   - `%{namespace}` — the pod's Kubernetes namespace
   - `%{container}` — the container directory name (kubelet stdout layout)
   - `%{metadata:<key>}` — a pod metadata value fetched via
     `singer.kubernetes.podMetadataFields`; the key is the last path segment of
     the configured field (e.g. field `labels:app` → `%{metadata:app}`)
2. **`topic`** — the required static fallback; legacy `\N` capture-group
   expansion against `logStreamRegex` still applies. Fallbacks are counted by
   the `singer.writer.topic_template_fallback` metric.

Pod-level identifiers (pod name, pod uid) are deliberately **not** supported as
template variables: they change on every restart/reschedule and would create
unbounded topic cardinality.

Examples:

```properties
# one topic per namespace+container
writer.kafka.topicTemplate=logs_%{namespace}_%{container}

# self-serve routing: teams set a label/annotation on their pods
# (requires singer.kubernetes.podMetadataFields to include e.g. labels:logTopic)
writer.kafka.topicTemplate=%{metadata:logTopic}
```

To restrict which pods a config applies to at all, combine with the pod
allowlist (`podAllowlist` in the log config plus
`singer.kubernetes.podAllowlistMetadataKey` in `singer.properties`).

## DaemonSet deployment notes

- Mount `/var/log/pods` (read-only) from the host.
- Mount a writable host path for Singer's watermark files so a restart or
  redeploy does not re-ship logs.
- For clusters where the kubelet read-only port (10255) is disabled, set
  `singer.kubernetes.useSecureConnection=true`,
  `singer.kubernetes.kubeletPort=10250`, point
  `serviceAccountTokenPath`/`serviceAccountCaCertPath` at the standard service
  account paths, expose the node IP as the `HOST_IP` environment variable via
  the downward API, and grant the service account `get` on `nodes/proxy`.
- Expose the Ostrich port (`singer.ostrichPort`) if you scrape metrics.
