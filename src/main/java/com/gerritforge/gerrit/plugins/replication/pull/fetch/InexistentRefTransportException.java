// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.internal.JGitText;

public class InexistentRefTransportException extends PermanentTransportException {
  private static final Pattern JGIT_INEXISTENT_REF_PATTERN =
      Pattern.compile(JGitText.get().remoteDoesNotHaveSpec.replaceAll("\\{0\\}", "([^\\\\s]+)"));
  private static final Pattern CGIT_INEXISTENT_REF_PATTERN =
      Pattern.compile(".*fatal.*couldn't find remote ref (.*)");

  private static final long serialVersionUID = 1L;
  private final String inexistentRef;

  public String getInexistentRef() {
    return inexistentRef;
  }

  public InexistentRefTransportException(String inexistentRef, Throwable cause) {
    super("Ref " + inexistentRef + " does not exist on remote", cause);

    this.inexistentRef = inexistentRef;
  }

  public static Optional<TransportException> getOptionalPermanentFailure(TransportException e) {
    Optional<TransportException> jgitEx = wrapException(JGIT_INEXISTENT_REF_PATTERN, e);
    if (jgitEx.isPresent()) {
      return jgitEx;
    }
    return wrapException(CGIT_INEXISTENT_REF_PATTERN, e);
  }

  private static Optional<TransportException> wrapException(
      Pattern exceptionPattern, TransportException exception) {
    Matcher exceptionMatcher = exceptionPattern.matcher(exception.getMessage());
    if (exceptionMatcher.matches()) {
      return Optional.of(new InexistentRefTransportException(exceptionMatcher.group(1), exception));
    }
    return Optional.empty();
  }
}
