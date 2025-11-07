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

package com.gerritforge.gerrit.plugins.replication.pull;

import static com.google.common.truth.Truth.assertThat;

import com.gerritforge.gerrit.plugins.replication.pull.fetch.InexistentRefTransportException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.PermanentTransportException;
import org.apache.sshd.common.SshException;
import org.eclipse.jgit.errors.TransportException;
import org.junit.Test;

public class PermanentFailureExceptionTest {

  @Test
  public void shouldConsiderSchUnknownHostAsPermanent() {
    assertThat(
            PermanentTransportException.wrapIfPermanentTransportException(
                new TransportException(
                    "SSH error",
                    new SshException(
                        "Failed (UnsupportedCredentialItem) to execute: some.commands"))))
        .isInstanceOf(PermanentTransportException.class);
  }

  @Test
  public void shouldConsiderNotExistingRefsFromJGitAsPermanent() {
    assertThat(
            PermanentTransportException.wrapIfPermanentTransportException(
                new TransportException("Remote does not have refs/heads/foo available for fetch.")))
        .isInstanceOf(InexistentRefTransportException.class);
  }

  @Test
  public void shouldConsiderNotExistingRefsFromCGitAsPermanent() {
    assertThat(
            PermanentTransportException.wrapIfPermanentTransportException(
                new TransportException(
                    "Cannot fetch from repo, error message: fatal: couldn't find remote ref"
                        + " refs/heads/foobranch")))
        .isInstanceOf(InexistentRefTransportException.class);
  }
}
