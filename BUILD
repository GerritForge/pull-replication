load("//tools/bzl:junit.bzl", "junit_tests")
load("//tools/bzl:plugin.bzl", "PLUGIN_DEPS", "PLUGIN_TEST_DEPS", "gerrit_plugin")

gerrit_plugin(
    name = "pull-replication",
    srcs = glob(["src/main/java/**/*.java"]),
    manifest_entries = [
        "Implementation-Title: Pull Replication plugin",
<<<<<<< PATCH SET (69319c2d603cec6fbbf52697a7c78b5d8c22b408 Update build URLs to point to GerritHub)
        "Implementation-URL: https://review.gerrithub.io/admin/projects/GerritForge/pull-replication",
=======
        "Implementation-URL: https://github.com/GerritForge/pull-replication",
>>>>>>> BASE      (004d934e64d73238e812503b2fb80d86941ec19e Update base package to com.gerritforge and add BSL license h)
        "Gerrit-PluginName: pull-replication",
        "Gerrit-Module: com.gerritforge.gerrit.plugins.replication.pull.PullReplicationModule",
        "Gerrit-InitStep: com.gerritforge.gerrit.plugins.replication.pull.InitPlugin",
        "Gerrit-SshModule: com.gerritforge.gerrit.plugins.replication.pull.SshModule",
        "Gerrit-HttpModule: com.gerritforge.gerrit.plugins.replication.pull.api.HttpModule",
        "Gerrit-ReloadMode: restart",
    ],
    resources = glob(["src/main/resources/**/*"]),
    deps = [
        ":events-broker-neverlink",
        ":healthcheck-neverlink",
        "//lib/commons:io",
        "//plugins/delete-project",
        "//plugins/replication",
        "@commons-lang3//jar",
    ],
)

junit_tests(
    name = "pull_replication_tests",
    size = "large",
    srcs = glob([
        "src/test/java/**/*Test.java",
    ]),
    tags = ["pull-replication"],
    visibility = ["//visibility:public"],
    deps = PLUGIN_TEST_DEPS + PLUGIN_DEPS + [
        ":pull-replication__plugin",
        ":pull_replication_util",
        "//plugins/delete-project",
        "//plugins/events-broker",
        "//plugins/healthcheck",
        "//plugins/replication",
    ],
)

[junit_tests(
    name = f[:f.index(".")].replace("/", "_"),
    srcs = [f],
    tags = ["pull-replication"],
    visibility = ["//visibility:public"],
    deps = PLUGIN_TEST_DEPS + PLUGIN_DEPS + [
        ":healthcheck-neverlink",
        ":pull-replication__plugin",
        ":pull_replication_util",
        "//plugins/replication",
    ],
) for f in glob(["src/test/java/**/*IT.java"])]

java_library(
    name = "pull_replication_util",
    testonly = True,
    srcs = glob(
        ["src/test/java/**/*.java"],
        exclude = [
            "src/test/java/**/*Test.java",
            "src/test/java/**/*IT.java",
        ],
    ),
    deps = PLUGIN_TEST_DEPS + PLUGIN_DEPS + [
        ":pull-replication__plugin",
        "//plugins/delete-project",
        "//plugins/healthcheck",
        "//plugins/replication",
    ],
)

java_library(
    name = "pull-replication__plugin_test_deps",
    testonly = 1,
    visibility = ["//visibility:public"],
    exports = PLUGIN_DEPS + PLUGIN_TEST_DEPS + [
        ":pull-replication__plugin",
        "//plugins/events-broker",
    ],
)

java_library(
    name = "events-broker-neverlink",
    neverlink = 1,
    exports = ["//plugins/events-broker"],
)

java_library(
    name = "healthcheck-neverlink",
    neverlink = 1,
    exports = ["//plugins/healthcheck"],
)
