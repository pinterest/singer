load("//bazel/rules:java_tests.bzl", "java_tests")

java_tests(
    srcs = glob(["*Test.java"]),
    prefix = "com.pinterest.singer_thrift.client",
    deps = [
        "@schemas//:schemas_thrift_singer_if",
        "//thrift-logger/src/main/java/com/pinterest/singer_thrift/client",
        "@ch_qos_logback_core",
        "@org_apache_thrift_libthrift",
    ],
    size = "small",
)
