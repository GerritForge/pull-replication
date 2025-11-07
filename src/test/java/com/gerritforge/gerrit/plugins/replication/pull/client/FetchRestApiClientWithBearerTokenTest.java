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

package com.gerritforge.gerrit.plugins.replication.pull.client;

import static com.google.common.truth.Truth.assertThat;
import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.plugins.replication.pull.filter.SyncRefsFilter;
import java.util.Optional;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FetchRestApiClientWithBearerTokenTest extends FetchRestApiClientBase {

  @Before
  public void setup() throws Exception {
    when(bearerTokenProvider.get()).thenReturn(Optional.of("some-bearer-token"));
    when(replicationConfig.getConfig()).thenReturn(config);
    when(config.getStringList("replication", null, "syncRefs")).thenReturn(new String[0]);
    HttpResult httpResult = new HttpResult(SC_CREATED, Optional.of("result message"));
    when(httpClient.execute(any(HttpRequestBase.class), any())).thenReturn(httpResult);
    when(httpClientFactory.create(any())).thenReturn(httpClient);

    syncRefsFilter = new SyncRefsFilter(replicationConfig);

    objectUnderTest =
        new FetchRestApiClient(
            credentials,
            httpClientFactory,
            replicationConfig,
            syncRefsFilter,
            pluginName,
            instanceId,
            bearerTokenProvider,
            source);
    verify(bearerTokenProvider).get();
  }

  @Override
  protected String urlAuthenticationPrefix() {
    return "";
  }

  @Override
  protected void assertAuthentication(HttpRequestBase httpRequest) {
    Header[] authorizationHeaders = httpRequest.getHeaders(HttpHeaders.AUTHORIZATION);
    assertThat(authorizationHeaders.length).isEqualTo(1);
    assertThat(authorizationHeaders[0].getValue()).isEqualTo("Bearer " + "some-bearer-token");
  }
}
