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
import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_RETRY_INTERVAL;
import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_RETRY_POLL_INTERVAL;
import static com.gerritforge.gerrit.plugins.replication.pull.event.EventsBrokerConsumerModule.STREAM_EVENTS_TOPIC_NAME;
import static java.util.Objects.requireNonNull;

import com.gerritforge.gerrit.eventbroker.AckAwareConsumer;
import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.BrokerApiPluginListener;
import com.gerritforge.gerrit.eventbroker.EventMetadata;
import com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgement;
import com.gerritforge.gerrit.plugins.replication.pull.ProjectLockFailureException;
import com.gerritforge.gerrit.plugins.replication.pull.ShutdownState;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RemoteConfigurationMissingException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.common.Nullable;
import com.google.gerrit.extensions.events.LifecycleListener;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.extensions.restapi.AuthException;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
import com.google.gerrit.server.config.GerritInstanceId;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.permissions.PermissionBackendException;
import com.google.gerrit.server.util.time.TimeUtil;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class EventsBrokerMessageConsumer
    implements AckAwareConsumer<Event>, LifecycleListener, BrokerApiPluginListener {
  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final DynamicItem<BrokerApi> eventsBrokerDi;
  private final StreamEventListener eventListener;
  private final ShutdownState shutdownState;
  private final String eventsTopicName;
  private final String groupId;
  private final String instanceId;
  private final int retryInterval;
  private final int retryPollInterval;
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
      @Nullable @Named(STREAM_EVENTS_GROUP_ID) String groupId,
      @Nullable @GerritInstanceId String instanceId,
      @Named(STREAM_EVENTS_RETRY_INTERVAL) int retryInterval,
      @Named(STREAM_EVENTS_RETRY_POLL_INTERVAL) int retryPollInterval) {

    this.eventsBrokerDi = eventsBroker;
    this.eventListener = eventListener;
    this.hasAllEventsPartition =
        eventsBrokerConfiguration
            .getPartitionsForTopic(eventsTopicName)
            .contains(ALL_EVENTS_PARTITION);
    this.shutdownState = shutdownState;
    this.eventsTopicName = eventsTopicName;
    this.groupId = groupId;
    this.instanceId =
        requireNonNull(
            Strings.emptyToNull(instanceId), "gerrit.instanceId cannot be null or empty");
    this.retryInterval = retryInterval;
    this.retryPollInterval = retryPollInterval;
  }

  @Override
  public void accept(Event event, EventMetadata metadata, MessageAcknowledgement<Event> msgAck)
      throws InterruptedException {
    if (!autoAck) {
      acceptSync(event, metadata, msgAck);
    } else {
      acceptAsync(event, msgAck);
    }
  }

  private void acceptSync(
      Event event, EventMetadata metadata, MessageAcknowledgement<Event> msgAck)
      throws InterruptedException {
    if (isRetryForOtherInstance(metadata)) {
      logger.atFine().log(
          "Skipping retry %d of broker event %s assigned to instance %s",
          metadata.retry().retryCount(), event.type, metadata.retry().retryingInstanceId());
      acknowledge(event, msgAck);
      return;
    }

    if (shouldThrottle(metadata)) {
      logger.atFine().log(
          "Deferring retry %d of broker event %s until its retry interval elapses",
          metadata.retry().retryCount(), event.type);
      requeueUnchangedAndAcknowledge(event, metadata, msgAck);
      return;
    }

    try {
      eventListener.fetchRefsForEventSync(event);
      msgAck.ack(event);
      if (shutdownState.isShuttingDown()) {
        stop();
      }
    } catch (ProjectLockFailureException | IOException e) {
      requeueAndAcknowledge(event, metadata, msgAck, e);
    } catch (AuthException
        | PermissionBackendException
        | UnprocessableEntityException
        | ResourceNotFoundException
        | RemoteConfigurationMissingException e) {
      logger.atSevere().withCause(e).log(
          "Failed to process broker event %s due to a non-transient error; leaving it"
              + " unacknowledged",
          event.type);
      throw new EventRejectedException(event, e);
    }
  }

  private boolean isRetryForOtherInstance(EventMetadata metadata) {
    return metadata.isRetry() && !instanceId.equals(metadata.retry().retryingInstanceId());
  }

  private boolean shouldThrottle(EventMetadata metadata) throws InterruptedException {
    if (!metadata.isRetry()) {
      return false;
    }

    long millisUntilRetry = retryInterval - (TimeUtil.nowMs() - metadata.retry().lastAttemptOn());
    if (millisUntilRetry <= 0) {
      return false;
    }

    Thread.sleep(Math.min(retryPollInterval, millisUntilRetry));
    return true;
  }

  private void requeueUnchangedAndAcknowledge(
      Event event, EventMetadata metadata, MessageAcknowledgement<Event> msgAck)
      throws InterruptedException {
    if (requeue(event, metadata)) {
      acknowledge(event, msgAck);
      return;
    }

    throw new EventRejectedException(
        event, new IllegalStateException("Failed to requeue throttled broker event"));
  }

  private void requeueAndAcknowledge(
      Event event,
      EventMetadata metadata,
      MessageAcknowledgement<Event> msgAck,
      Exception processingFailure)
      throws InterruptedException {
    EventMetadata retryMetadata = metadata.nextRetry(instanceId);
    logger.atWarning().withCause(processingFailure).log(
        "Failed to process broker event %s; attempting retry %d",
        event.type, retryMetadata.retry().retryCount());

    if (requeue(event, retryMetadata)) {
      acknowledge(event, msgAck);
      return;
    }

    throw new EventRejectedException(event, processingFailure);
  }

  private void acknowledge(Event event, MessageAcknowledgement<Event> msgAck) {
    msgAck.ack(event);
    if (shutdownState.isShuttingDown()) {
      stop();
    }
  }

  private boolean requeue(Event event, EventMetadata metadata) throws InterruptedException {
    try {
      return eventsBrokerDi.get().requeue(eventsTopicName, event, metadata).get();
    } catch (ExecutionException e) {
      logger.atSevere().withCause(e).log("Failed to requeue broker event %s", event.type);
      return false;
    }
  }

  private void acceptAsync(Event event, MessageAcknowledgement<Event> msgAck)
      throws InterruptedException {
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
        | ResourceNotFoundException
        | RemoteConfigurationMissingException e) {
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
    if (!autoAck) {
      Preconditions.checkState(
          hasAllEventsPartition, "The all-events partition is required when manual ack is enabled");
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

  @Override
  public void beforeBrokerApiStopped() {
    Preconditions.checkState(brokerApiImplPluginStarted, "BrokerApi implementation plugin cannot be stopped more than once");
    brokerApiImplPluginStarted = false;
  }

  @Override
  public void stop() {
    shutdownState.setIsShuttingDown(true);
    eventsBrokerDi.get().disconnect(eventsTopicName, groupId);
  }
}
