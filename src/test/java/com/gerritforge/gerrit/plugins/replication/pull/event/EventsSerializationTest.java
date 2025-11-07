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

package com.gerritforge.gerrit.plugins.replication.pull.event;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Objects;
import com.google.gerrit.server.events.EventGsonProvider;
import com.google.gson.Gson;
import com.gerritforge.gerrit.plugins.replication.pull.FetchRefReplicatedEvent;
import com.gerritforge.gerrit.plugins.replication.pull.FetchReplicationScheduledEvent;
import com.gerritforge.gerrit.plugins.replication.pull.ReplicationState;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.transport.URIish;
import org.junit.Before;
import org.junit.Test;

public class EventsSerializationTest {
  private static URIish sourceUri;
  private static final Gson eventGson = new EventGsonProvider().get();
  private static final String TEST_PROJECT = "test_project";
  private static final String TEST_REF = "refs/heads/master";

  @Before
  public void setUp() throws Exception {
    sourceUri = new URIish(String.format("git://aSourceNode/%s.git", TEST_PROJECT));
  }

  @Test
  public void shouldSerializeFetchRefReplicatedEvent() {
    FetchRefReplicatedEvent origEvent =
        new FetchRefReplicatedEvent(
            TEST_PROJECT,
            TEST_REF,
            sourceUri,
            ReplicationState.RefFetchResult.SUCCEEDED,
            RefUpdate.Result.FAST_FORWARD);

    assertThat(origEvent)
        .isEqualTo(eventGson.fromJson(eventGson.toJson(origEvent), FetchRefReplicatedEvent.class));
  }

  @Test
  public void shouldSerializeFetchReplicationScheduledEvent() {
    FetchReplicationScheduledEvent origEvent =
        new FetchReplicationScheduledEvent(TEST_PROJECT, TEST_REF, sourceUri);

    assertTrue(
        equals(
            origEvent,
            eventGson.fromJson(eventGson.toJson(origEvent), FetchReplicationScheduledEvent.class)));
  }

  private boolean equals(FetchReplicationScheduledEvent scheduledEvent, Object other) {
    if (!(other instanceof FetchReplicationScheduledEvent)) {
      return false;
    }
    FetchReplicationScheduledEvent event = (FetchReplicationScheduledEvent) other;
    if (!Objects.equal(event.project, scheduledEvent.project)) {
      return false;
    }
    if (!Objects.equal(event.ref, scheduledEvent.ref)) {
      return false;
    }
    if (!Objects.equal(event.targetUri, scheduledEvent.targetUri)) {
      return false;
    }
    return Objects.equal(event.status, scheduledEvent.status);
  }
}
