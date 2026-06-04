load(
    "@com_googlesource_gerrit_bazlets//:gerrit_plugin.bzl",
    "gerrit_plugin",
    "gerrit_plugin_test_util",
    "gerrit_plugin_tests",
)
load("@rules_java//java:defs.bzl", "java_library")

PLUGIN = "pull-replication"

TEST_DEPS = [
    "//plugins/delete-project",
    "//plugins/healthcheck",
    "//plugins/replication",
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
        "//plugins/delete-project",
        "//plugins/replication",
    ],
)

gerrit_plugin_tests(
    size = "large",
    srcs = glob(["src/test/java/**/*Test.java"]),
    plugin = PLUGIN,
    deps = [
        ":pull-replication_test_util",
        "//plugins/events-broker",
    ],
)

[gerrit_plugin_tests(
    name = f[:f.index(".")].replace("/", "_"),
    srcs = [f],
    plugin = PLUGIN,
    deps = [
        ":healthcheck-neverlink",
        ":pull-replication_test_util",
    ],
) for f in glob(["src/test/java/**/*IT.java"])]

gerrit_plugin_test_util(
    name = "pull-replication_test_util",
    srcs = glob(
        ["src/test/java/**/*.java"],
        exclude = [
            "src/test/java/**/*Test.java",
            "src/test/java/**/*IT.java",
        ],
    ),
    plugin = PLUGIN,
    exports = TEST_DEPS,
    deps = TEST_DEPS,
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
