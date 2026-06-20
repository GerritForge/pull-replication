// Copyright (C) 2026 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull.api;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.http.HttpStatus.SC_OK;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RefUpdateException;
import com.google.common.collect.Lists;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.Response;
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
import com.google.gerrit.server.project.ProjectResource;
import java.util.Collections;
import java.util.List;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.RefUpdate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BatchApplyObjectActionTest {

  private static final long DUMMY_EVENT_TIMESTAMP = 1684875939;
  private static final String LABEL = "instance-2-label";
  private static final String REF_NAME = "refs/heads/master";
  private static final String SAMPLE_COMMIT_OBJECT_ID = "9f8d52853089a3cf00c02ff7bd0817bd4353a95a";
  private static final String SAMPLE_TREE_OBJECT_ID = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

  private static final String SAMPLE_COMMIT_CONTENT =
      "tree "
          + SAMPLE_TREE_OBJECT_ID
          + "\n"
          + "parent 20eb48d28be86dc88fb4bef747f08de0fbefe12d\n"
          + "author Gerrit User 1000000 <1000000@69ec38f0-350e-4d9c-96d4-bc956f2faaac> 1610471611"
          + " +0100\n"
          + "committer Gerrit Code Review <root@maczech-XPS-15> 1610471611 +0100\n"
          + "\n"
          + "Update patch set 1\n";

  private static final Project.NameKey PROJECT = Project.nameKey("aProject");

  private BatchApplyObjectAction batchApplyObjectAction;

  @Mock private ApplyObjectCommand applyObjectCommand;
  @Mock private FetchPreconditions preConditions;
  @Mock private ProjectResource projectResource;

  @Before
  public void setup() throws Exception {
    when(projectResource.getNameKey()).thenReturn(PROJECT);
    when(preConditions.canCallFetchApi()).thenReturn(true);
    batchApplyObjectAction =
        new BatchApplyObjectAction(
            applyObjectCommand, new BatchApplyObjectInputValidator(preConditions));
  }

  @Test
  public void shouldCallApplyObjectCommandForEveryRevision() throws Exception {
    RevisionInput first =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());
    RevisionInput second =
        new RevisionInput(
            LABEL, "refs/heads/other", DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    batchApplyObjectAction.apply(projectResource, List.of(first, second));

    verify(applyObjectCommand)
        .applyObject(
            eq(PROJECT),
            eq(REF_NAME),
            eq(first.getRevisionData()),
            eq(LABEL),
            eq(DUMMY_EVENT_TIMESTAMP));
    verify(applyObjectCommand)
        .applyObject(
            eq(PROJECT),
            eq("refs/heads/other"),
            eq(second.getRevisionData()),
            eq(LABEL),
            eq(DUMMY_EVENT_TIMESTAMP));
  }

  @Test
  public void shouldReturnOkResponseCodeWhenAllRevisionsAreProcessedSuccessfully()
      throws RestApiException {
    RevisionInput first =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());
    RevisionInput second =
        new RevisionInput(
            LABEL, "refs/heads/other", DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    Response<?> response = batchApplyObjectAction.apply(projectResource, List.of(first, second));

    assertThat(response.statusCode()).isEqualTo(SC_OK);
  }

  @Test(expected = UnprocessableEntityException.class)
  public void shouldThrowOnFirstFailure() throws Exception {
    RevisionInput bad =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    doThrow(new RefUpdateException(RefUpdate.Result.LOCK_FAILURE, "BOOM"))
        .when(applyObjectCommand)
        .applyObject(any(), any(), any(), any(), anyLong());

    batchApplyObjectAction.apply(projectResource, List.of(bad));
  }

  @Test
  public void shouldStopProcessingWhenAFailureOccurs() throws Exception {
    RevisionInput bad =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());
    RevisionInput good =
        new RevisionInput(
            LABEL, "refs/heads/other", DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    doThrow(new RefUpdateException(RefUpdate.Result.LOCK_FAILURE, "BOOM"))
        .when(applyObjectCommand)
        .applyObject(eq(PROJECT), eq(REF_NAME), any(), any(), anyLong());

    try {
      batchApplyObjectAction.apply(projectResource, List.of(bad, good));
    } catch (RestApiException ignored) {
      // expected
    }

    verify(applyObjectCommand, times(1)).applyObject(any(), any(), any(), any(), anyLong());
  }

  private RevisionData createSampleRevisionData() {
    RevisionObjectData commitData =
        new RevisionObjectData(
            SAMPLE_COMMIT_OBJECT_ID, Constants.OBJ_COMMIT, SAMPLE_COMMIT_CONTENT.getBytes());
    RevisionObjectData treeData =
        new RevisionObjectData(SAMPLE_TREE_OBJECT_ID, Constants.OBJ_TREE, new byte[] {});
    return new RevisionData(Collections.emptyList(), commitData, treeData, Lists.newArrayList());
  }
}
