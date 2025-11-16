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

import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_GROUP_ID;
import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_TOPIC_NAME;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.plugins.replication.pull.ShutdownState;
import com.google.gerrit.common.Nullable;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.extensions.restapi.AuthException;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.permissions.PermissionBackendException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.io.IOException;
import java.util.function.Consumer;

public class EventsBrokerMessageConsumer implements Consumer<Event>, LifecycleListener {
  private final DynamicItem<BrokerApi> eventsBrokerDi;
  private final StreamEventListener eventListener;
  private final ShutdownState shutdownState;
  private final String eventsTopicName;
  private final String groupId;

  @Inject
  public EventsBrokerMessageConsumer(
      DynamicItem<BrokerApi> eventsBroker,
      StreamEventListener eventListener,
      ShutdownState shutdownState,
      @Named(STREAM_EVENTS_TOPIC_NAME) String eventsTopicName,
      @Nullable @Named(STREAM_EVENTS_GROUP_ID) String groupId) {

    this.eventsBrokerDi = eventsBroker;
    this.eventListener = eventListener;
    this.shutdownState = shutdownState;
    this.eventsTopicName = eventsTopicName;
    this.groupId = groupId;
  }

  @Override
  public void accept(Event event) {
    try {
      eventListener.fetchRefsForEvent(event);
      if (shutdownState.isShuttingDown()) stop();
    } catch (AuthException
        | PermissionBackendException
        | IOException
        | UnprocessableEntityException
        | ResourceNotFoundException e) {
      throw new EventRejectedException(event, e);
    }
  }

  @Override
  public void start() {
    BrokerApi brokerApi = eventsBrokerDi.get();
    if (groupId == null) {
      brokerApi.receiveAsync(eventsTopicName, this);
      return;
    }
    brokerApi.receiveAsync(eventsTopicName, groupId, this);
  }

  @Override
  public void stop() {
    shutdownState.setIsShuttingDown(true);
    eventsBrokerDi.get().disconnect(eventsTopicName, groupId);
  }
}
