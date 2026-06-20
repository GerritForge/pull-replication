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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.BatchRefUpdateState;
import com.google.common.collect.Lists;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.BadRequestException;
import com.google.gerrit.extensions.restapi.Response;
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
import com.google.gerrit.server.project.ProjectResource;
import java.util.Collections;
import java.util.List;
import org.eclipse.jgit.lib.BatchRefUpdate;
import org.eclipse.jgit.lib.Constants;
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

  @Mock private BatchApplyObjectCommand batchApplyObjectCommand;
  @Mock private FetchPreconditions preConditions;
  @Mock private ProjectResource projectResource;
  @Mock private BatchRefUpdate bru;

  @Before
  public void setup() throws Exception {
    when(projectResource.getNameKey()).thenReturn(PROJECT);
    when(preConditions.canCallFetchApi()).thenReturn(true);
    batchApplyObjectAction =
        new BatchApplyObjectAction(
            batchApplyObjectCommand, new BatchApplyObjectInputValidator(preConditions));
  }

  @Test
  public void shouldCallBatchApplyObjectsOnce() throws Exception {
    RevisionInput first =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());
    RevisionInput second =
        new RevisionInput(
            LABEL, "refs/heads/other", DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    batchApplyObjectAction.apply(projectResource, List.of(first, second));

    verify(batchApplyObjectCommand)
        .batchApplyObjects(
            eq("Batch Apply object"),
            eq(PROJECT),
            eq(List.of(REF_NAME, "refs/heads/other")),
            any(),
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

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionWhenInputIsEmpty() throws RestApiException {
    batchApplyObjectAction.apply(projectResource, Collections.emptyList());
  }

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionWhenLabelsDiffer() throws RestApiException {
    RevisionInput first =
        new RevisionInput("label-a", REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());
    RevisionInput second =
        new RevisionInput(
            "label-b", "refs/heads/other", DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    batchApplyObjectAction.apply(projectResource, List.of(first, second));
  }

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionWhenEventTimestampsDiffer() throws RestApiException {
    RevisionInput first =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());
    RevisionInput second =
        new RevisionInput(
            LABEL, "refs/heads/other", DUMMY_EVENT_TIMESTAMP + 1, createSampleRevisionData());

    batchApplyObjectAction.apply(projectResource, List.of(first, second));
  }

  @Test(expected = UnprocessableEntityException.class)
  public void shouldThrowUnprocessableEntityOnBatchRefUpdateFailure() throws Exception {
    RevisionInput input =
        new RevisionInput(LABEL, REF_NAME, DUMMY_EVENT_TIMESTAMP, createSampleRevisionData());

    BatchRefUpdateException toThrow =
        new BatchRefUpdateException(new BatchRefUpdateState(bru), "BOOM");
    doThrow(toThrow)
        .when(batchApplyObjectCommand)
        .batchApplyObjects(any(), any(), any(), any(), any(), anyLong());

    batchApplyObjectAction.apply(projectResource, List.of(input));
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
