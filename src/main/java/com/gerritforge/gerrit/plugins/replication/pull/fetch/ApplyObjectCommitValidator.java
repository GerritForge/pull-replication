package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import java.io.IOException;
import java.util.function.Consumer;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;

@FunctionalInterface
interface ApplyObjectCommitValidator {
  boolean isValid(Repository repo, String refName, RevCommit commit, Consumer<String> errorCallback)
      throws IOException;
}
