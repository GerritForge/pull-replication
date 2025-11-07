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

package com.gerritforge.gerrit.plugins.replication.pull.api.util;

import static javax.servlet.http.HttpServletResponse.SC_CREATED;
import static javax.servlet.http.HttpServletResponse.SC_OK;

import com.google.common.flogger.FluentLogger;
import com.google.gerrit.extensions.api.projects.HeadInput;
import com.google.gerrit.extensions.restapi.BadRequestException;
import com.google.gerrit.extensions.restapi.Response;
import com.google.gerrit.httpd.restapi.RestApiServlet;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.inject.TypeLiteral;
import com.gerritforge.gerrit.plugins.replication.pull.api.FetchAction;
import com.gerritforge.gerrit.plugins.replication.pull.api.HttpPayloadGsonProvider;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionsInput;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PayloadSerDes {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private static final Gson gson = HttpPayloadGsonProvider.get();

  public static RevisionInput parseRevisionInput(HttpServletRequest httpRequest)
      throws BadRequestException, IOException {
    return parse(httpRequest, TypeLiteral.get(RevisionInput.class));
  }

  public static RevisionsInput parseRevisionsInput(HttpServletRequest httpRequest)
      throws BadRequestException, IOException {
    return parse(httpRequest, TypeLiteral.get(RevisionsInput.class));
  }

  public static HeadInput parseHeadInput(HttpServletRequest httpRequest)
      throws BadRequestException, IOException {
    return parse(httpRequest, TypeLiteral.get(HeadInput.class));
  }

  public static FetchAction.Input parseInput(HttpServletRequest httpRequest)
      throws BadRequestException, IOException {
    return parse(httpRequest, TypeLiteral.get(FetchAction.Input.class));
  }

  public static <T> void writeResponse(HttpServletResponse httpResponse, Response<T> response)
      throws IOException {
    String responseJson = gson.toJson(response);
    if (response.statusCode() == SC_OK || response.statusCode() == SC_CREATED) {

      httpResponse.setContentType("application/json");
      httpResponse.setStatus(response.statusCode());
      PrintWriter writer = httpResponse.getWriter();
      writer.print(new String(RestApiServlet.JSON_MAGIC));
      writer.print(responseJson);
    } else {
      httpResponse.sendError(response.statusCode(), responseJson);
    }
  }

  private static <T> T parse(HttpServletRequest httpRequest, TypeLiteral<T> typeLiteral)
      throws IOException, BadRequestException {

    try (BufferedReader br = httpRequest.getReader();
        JsonReader json = new JsonReader(br)) {
      try {
        json.setLenient(true);

        try {
          json.peek();
        } catch (EOFException e) {
          throw new BadRequestException("Expected JSON object", e);
        }

        return gson.fromJson(json, typeLiteral.getType());
      } finally {
        try {
          // Reader.close won't consume the rest of the input. Explicitly consume the request
          // body.
          br.skip(Long.MAX_VALUE);
        } catch (Exception e) {
          // ignore, e.g. trying to consume the rest of the input may fail if the request was
          // cancelled
          logger.atFine().withCause(e).log("Exception during the parsing of the request json");
        }
      }
    }
  }
}
