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

import static com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration.ALL_EVENTS_PARTITION;
import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_GROUP_ID;
import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_TOPIC_NAME;

import com.gerritforge.gerrit.eventbroker.AckAwareConsumer;
import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.BrokerApiPluginListener;
import com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgement;
import com.gerritforge.gerrit.plugins.replication.pull.ProjectLockFailureException;
import com.gerritforge.gerrit.plugins.replication.pull.ShutdownState;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RemoteConfigurationMissingException;
import com.google.common.base.Preconditions;
import com.google.common.flogger.FluentLogger;
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

public class EventsBrokerMessageConsumer
    implements AckAwareConsumer<Event>, LifecycleListener, BrokerApiPluginListener {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final DynamicItem<BrokerApi> eventsBrokerDi;
  private final StreamEventListener eventListener;
  private final ShutdownState shutdownState;
  private final String eventsTopicName;
  private final String groupId;
  private final boolean hasAllEventsPartition;
  private boolean autoAck;
  private volatile boolean brokerApiImplPluginStarted;

  @Inject
  public EventsBrokerMessageConsumer(
      DynamicItem<BrokerApi> eventsBroker,
      StreamEventListener eventListener,
      EventsBrokerConfiguration eventsBrokerConfiguration,
      ShutdownState shutdownState,
      @Named(STREAM_EVENTS_TOPIC_NAME) String eventsTopicName,
      @Nullable @Named(STREAM_EVENTS_GROUP_ID) String groupId) {

    this.eventsBrokerDi = eventsBroker;
    this.eventListener = eventListener;
    this.hasAllEventsPartition =
        eventsBrokerConfiguration
            .getPartitionsForTopic(eventsTopicName)
            .contains(ALL_EVENTS_PARTITION);
    this.shutdownState = shutdownState;
    this.eventsTopicName = eventsTopicName;
    this.groupId = groupId;
  }

  @Override
  public void accept(Event event, MessageAcknowledgement<Event> msgAck) {
    if (ackAfterFetch()) {
      acceptSync(event, msgAck);
    } else {
      acceptAsync(event, msgAck);
    }
  }

  private void acceptSync(Event event, MessageAcknowledgement<Event> msgAck) {
    try {
      eventListener.fetchRefsForEventSync(event);
      msgAck.ack(event);
      if (shutdownState.isShuttingDown()) stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.atSevere().withCause(e).log(
          "Interrupted while processing broker event %s; leaving it unacknowledged", event.type);
      // TODO: Message might be reprocessed, in case of restart without acking of the message or any
      // subsequent one.
      // A better approach should retry/requeue the fetch.
      throw new EventRejectedException(event, e);
    } catch (AuthException
        | PermissionBackendException
        | ProjectLockFailureException
        | IOException
        | UnprocessableEntityException
        | ResourceNotFoundException
        | RemoteConfigurationMissingException e) {

      logger.atSevere().withCause(e).log(
          "Failed to process broker event %s; leaving it unacknowledged", event.type);
      // TODO: Message might be reprocessed, in case of restart without acking of the message or any
      // subsequent one.
      // A better approach should retry/requeue the fetch.
      throw new EventRejectedException(event, e);
    }
  }

  private void acceptAsync(Event event, MessageAcknowledgement<Event> msgAck) {
    try {
      eventListener.fetchRefsForEventAsync(event);
      if (!autoAck) {
        msgAck.ack(event);
      }
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
    if (isBrokerApiBound()) {
      onBrokerApiStarted();
    } else {
      logger.atInfo().log(
          "No broker plugin bound, not starting consumers yet for topic: %s", eventsTopicName);
    }
  }

  @Override
  public DynamicItem<BrokerApi> brokerApiDynamicItem() {
    return eventsBrokerDi;
  }

  @Override
  public void onBrokerApiStarted() {
    Preconditions.checkState(!brokerApiImplPluginStarted, "BrokerApi implementation plugin cannot be started more than once");
    logger.atInfo().log("Starting consumers for topic: %s", eventsTopicName);
    BrokerApi brokerApi = eventsBrokerDi.get();
    this.autoAck = brokerApi.isAutoAck();
    if (ackAfterFetch()) {
      Preconditions.checkState(
          groupId != null,
          "replication.eventBrokerGroupId is required for the all events partition consumer");
      brokerApi.receiveAsyncWithPartition(eventsTopicName, ALL_EVENTS_PARTITION, groupId, this);
    } else if (groupId == null) {
      brokerApi.receiveAsync(eventsTopicName, this);
    } else {
      brokerApi.receiveAsync(eventsTopicName, groupId, this);
    }
    brokerApiImplPluginStarted = true;
  }

  private boolean ackAfterFetch() {
    return hasAllEventsPartition && !autoAck;
  }

  @Override
  public void beforeBrokerApiStopped() {
    Preconditions.checkState(
        brokerApiImplPluginStarted,
        "BrokerApi implementation plugin cannot be stopped more than once");
    brokerApiImplPluginStarted = false;
  }

  @Override
  public void stop() {
    shutdownState.setIsShuttingDown(true);
    eventsBrokerDi.get().disconnect(eventsTopicName, groupId);
  }
}
