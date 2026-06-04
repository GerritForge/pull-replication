load(
    "@com_googlesource_gerrit_bazlets//:gerrit_plugin.bzl",
    "gerrit_plugin",
    "gerrit_plugin_tests",
)
load("@rules_java//java:defs.bzl", "java_library")
load("//tools/bzl:junit.bzl", "junit_tests")

PLUGIN = "pull-replication"

# Constants inlined from //tools/bzl:plugin.bzl to remove legacy dependency.
PLUGIN_DEPS = ["//plugins:plugin-lib"]

PLUGIN_TEST_DEPS = [
    "//java/com/google/gerrit/acceptance:lib",
    "//lib/bouncycastle:bcpg",
    "//lib/bouncycastle:bcpkix",
    "//lib/bouncycastle:bcprov",
]

gerrit_plugin(
    srcs = glob(["src/main/java/**/*.java"]),
    manifest_entries = [
        "Implementation-Title: Pull Replication plugin",
        "Implementation-URL: https://github.com/GerritForge/pull-replication",
        "Gerrit-PluginName: pull-replication",
        "Gerrit-Module: com.gerritforge.gerrit.plugins.replication.pull.PullReplicationModule",
        "Gerrit-InitStep: com.gerritforge.gerrit.plugins.replication.pull.InitPlugin",
        "Gerrit-SshModule: com.gerritforge.gerrit.plugins.replication.pull.SshModule",
        "Gerrit-HttpModule: com.gerritforge.gerrit.plugins.replication.pull.api.HttpModule",
        "Gerrit-ReloadMode: restart",
    ],
    plugin = PLUGIN,
    resources = glob(["src/main/resources/**/*"]),
    deps = [
        ":events-broker-neverlink",
        ":healthcheck-neverlink",
        "//lib/commons:io",
        "//plugins/delete-project",
        "//plugins/replication",
    ],
)

gerrit_plugin_tests(
    name = "pull_replication_tests",
    size = "large",
    srcs = glob(["src/test/java/**/*Test.java"]),
    plugin = PLUGIN,
    tags = ["pull-replication"],
    visibility = ["//visibility:public"],
    deps = [
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
    name = "events-broker-neverlink",
    neverlink = 1,
    exports = ["//plugins/events-broker"],
)

java_library(
    name = "healthcheck-neverlink",
    neverlink = 1,
    exports = ["//plugins/healthcheck"],
)