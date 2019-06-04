load("//bazel/rules:checkstyle.bzl", "checkstyle")

checkstyle(
    name = "checkstyle",
    srcs= glob(["**/*.java"]),
    suppression="checkstyle-suppressions.xml",
)

java_library(
    name = "client",
    srcs = glob(["**/*.java"]),
    deps = [
        "@schemas//:schemas_thrift_singer_if",
        "//commons/src/main/java/com/pinterest/commons/util",
        "//commons/src/main/java/com/pinterest/commons/ostrich",
        "//commons/src/main/java/com/pinterest/commons/thrift",
        "@ch_qos_logback_core",
        "@com_google_guava",
        "@org_apache_thrift_libthrift",
        "@org_slf4j_api",
    ],
    visibility = ["//visibility:public"],
)
