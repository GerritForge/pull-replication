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

package com.gerritforge.gerrit.plugins.replication.pull.api;

import static com.google.gerrit.acceptance.AbstractPredicateTest.GSON;
import static com.google.gerrit.acceptance.testsuite.project.TestProjectUpdate.allowCapability;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionsInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionsInput;
import com.google.common.net.MediaType;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.acceptance.testsuite.project.ProjectOperations;
import com.google.gerrit.common.data.GlobalCapability;
import com.google.gerrit.extensions.restapi.Url;
import com.google.gerrit.server.group.SystemGroupBackend;
import com.google.inject.Inject;
import java.util.Collections;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.eclipse.jgit.lib.Constants;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class ProjectInitializationActionIT extends ActionITBase {
  public static final String INVALID_TEST_PROJECT_NAME = "\0";
  @Inject private ProjectOperations projectOperations;

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void shouldReturnUnauthorizedForUserWithoutPermissions() throws Exception {
    httpClientFactory
        .create(source)
        .execute(
            createPutRequestWithHeaders(),
            assertHttpResponseCode(HttpServletResponse.SC_UNAUTHORIZED));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void shouldReturnBadRequestIfContentNotSet() throws Exception {
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsAdmin(createPutRequestWithoutHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_BAD_REQUEST));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void shouldCreateRepository() throws Exception {
    String newProjectName = "new/newProjectForReplica" + System.currentTimeMillis();
    url = getURLWithAuthenticationPrefix(newProjectName);
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsAdmin(createPutRequestWithHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_CREATED));

    HttpRequestBase getNewProjectRequest =
        withBasicAuthenticationAsAdmin(
            new HttpGet(userRestSession.url() + "/a/projects/" + Url.encode(newProjectName)));

    httpClientFactory
        .create(source)
        .execute(getNewProjectRequest, assertHttpResponseCode(HttpServletResponse.SC_OK));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void shouldCreateRepositoryWhenUserHasProjectCreationCapabilities() throws Exception {
    String newProjectName = "new/newProjectForReplica" + System.currentTimeMillis();
    url = getURLWithAuthenticationPrefix(newProjectName);
    HttpRequestBase put = withBasicAuthenticationAsUser(createPutRequestWithHeaders());
    httpClientFactory
        .create(source)
        .execute(put, assertHttpResponseCode(HttpServletResponse.SC_FORBIDDEN));

    projectOperations
        .project(allProjects)
        .forUpdate()
        .add(
            allowCapability(GlobalCapability.CREATE_PROJECT)
                .group(SystemGroupBackend.REGISTERED_USERS))
        .update();

    httpClientFactory
        .create(source)
        .execute(put, assertHttpResponseCode(HttpServletResponse.SC_CREATED));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void shouldReturnForbiddenIfUserNotAuthorized() throws Exception {
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsUser(createPutRequestWithHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_FORBIDDEN));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  public void shouldCreateRepositoryWhenNodeIsAReplica() throws Exception {
    String newProjectName = "new/newProjectForReplica";
    url = getURLWithAuthenticationPrefix(newProjectName);
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsAdmin(createPutRequestWithHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_CREATED));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  public void shouldReturnForbiddenIfUserNotAuthorizedAndNodeIsAReplica() throws Exception {
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsUser(createPutRequestWithHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_FORBIDDEN));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  public void shouldCreateRepositoryWhenUserHasProjectCreationCapabilitiesAndNodeIsAReplica()
      throws Exception {
    String newProjectName = "new/newProjectForUserWithCapabilitiesReplica";
    url = getURLWithAuthenticationPrefix(newProjectName);
    HttpRequestBase put = withBasicAuthenticationAsUser(createPutRequestWithHeaders());
    httpClientFactory
        .create(source)
        .execute(put, assertHttpResponseCode(HttpServletResponse.SC_FORBIDDEN));

    projectOperations
        .project(allProjects)
        .forUpdate()
        .add(
            allowCapability(GlobalCapability.CREATE_PROJECT)
                .group(SystemGroupBackend.REGISTERED_USERS))
        .update();

    httpClientFactory
        .create(source)
        .execute(put, assertHttpResponseCode(HttpServletResponse.SC_CREATED));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  public void shouldReturnBadRequestIfProjectNameIsInvalidAndCannotBeCreatedWhenNodeIsAReplica()
      throws Exception {
    url = getURLWithAuthenticationPrefix(INVALID_TEST_PROJECT_NAME);
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsAdmin(createPutRequestWithHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_BAD_REQUEST));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  public void shouldReturnBadRequestIfContentNotSetWhenNodeIsAReplica() throws Exception {
    httpClientFactory
        .create(source)
        .execute(
            withBasicAuthenticationAsAdmin(createPutRequestWithoutHeaders()),
            assertHttpResponseCode(HttpServletResponse.SC_BAD_REQUEST));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  public void shouldReturnUnauthorizedForUserWithoutPermissionsWhenNodeIsAReplica()
      throws Exception {
    httpClientFactory
        .create(source)
        .execute(
            createPutRequestWithHeaders(),
            assertHttpResponseCode(HttpServletResponse.SC_UNAUTHORIZED));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "true")
  @GerritConfig(name = "auth.bearerToken", value = "some-bearer-token")
  public void shouldCreateRepositoryWhenNodeIsAReplicaWithBearerToken() throws Exception {
    String newProjectName = "new/newProjectForReplica" + System.currentTimeMillis();
    url = getURLWithoutAuthenticationPrefix(newProjectName);
    httpClientFactory
        .create(source)
        .execute(
            withBearerTokenAuthentication(createPutRequestWithHeaders(), "some-bearer-token"),
            assertHttpResponseCode(HttpServletResponse.SC_CREATED));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  @GerritConfig(name = "container.replica", value = "false")
  @GerritConfig(name = "auth.bearerToken", value = "some-bearer-token")
  public void shouldCreateRepositoryWhenNodeIsAPrimaryWithBearerToken() throws Exception {
    String newProjectName = "new/newProjectForReplica" + System.currentTimeMillis();
    url = getURLWithoutAuthenticationPrefix(newProjectName);
    httpClientFactory
        .create(source)
        .execute(
            withBearerTokenAuthentication(createPutRequestWithHeaders(), "some-bearer-token"),
            assertHttpResponseCode(HttpServletResponse.SC_CREATED));
  }

  @Override
  protected String getURLWithAuthenticationPrefix(String projectName) {
    return userRestSession.url()
        + String.format("/a/plugins/pull-replication/init-project/%s.git", Url.encode(projectName));
  }

  protected HttpPut createPutRequestWithHeaders() {
    HttpPut put = createPutRequestWithoutHeaders();
    put.addHeader(new BasicHeader("Accept", MediaType.ANY_TEXT_TYPE.toString()));
    put.addHeader(new BasicHeader("Content-Type", MediaType.JSON_UTF_8.toString()));
    String sampleTreeObjectId = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

    String sampleCommitContent =
        "tree "
            + sampleTreeObjectId
            + "\n"
            + "author Gerrit User 1000000 <1000000@69ec38f0-350e-4d9c-96d4-bc956f2faaac> 1610471611"
            + " +0100\n"
            + "committer Gerrit Code Review <root@maczech-XPS-15> 1610471611 +0100\n"
            + "\n"
            + "Update patch set 1\n"
            + "\n"
            + "Change has been successfully merged by Administrator\n"
            + "\n"
            + "Patch-set: 1\n"
            + "Status: merged\n"
            + "Tag: autogenerated:gerrit:merged\n"
            + "Reviewer: Gerrit User 1000000 <1000000@69ec38f0-350e-4d9c-96d4-bc956f2faaac>\n"
            + "Label: SUBM=+1\n"
            + "Submission-id: 1904-1610471611558-783c0a2f\n"
            + "Submitted-with: OK\n"
            + "Submitted-with: OK: Code-Review: Gerrit User 1000000"
            + " <1000000@69ec38f0-350e-4d9c-96d4-bc956f2faaac>";
    RevisionObjectData commitObject =
        new RevisionObjectData(
            "d582c7695b04124999699797176475b4db9b25b6", Constants.OBJ_COMMIT, sampleCommitContent.getBytes());
    RevisionObjectData treeObject =
        new RevisionObjectData(
            sampleTreeObjectId, Constants.OBJ_TREE, new byte[0]);
    RevisionData revisionData =
        new RevisionData(Collections.emptyList(), commitObject, treeObject, Collections.emptyList());
    RevisionsInput revisionsInput =
        new RevisionsInput(
            "someLabel",
            "refs/meta/config",
            System.currentTimeMillis(),
            new RevisionData[] {revisionData});
    put.setEntity(new StringEntity(GSON.toJson(revisionsInput), StandardCharsets.UTF_8));
    return put;
  }

  protected HttpPut createPutRequestWithoutHeaders() {
    HttpPut put = new HttpPut(url);
    return put;
  }
}
