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
import static com.google.common.truth.Truth.assertThat;
import static com.google.gerrit.testing.GerritJUnit.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.eventbroker.BrokerApi;
import com.gerritforge.gerrit.eventbroker.EventsBrokerConfiguration;
import com.gerritforge.gerrit.eventbroker.MessageAcknowledgement;
import com.gerritforge.gerrit.eventbroker.MessageMetadata;
import com.gerritforge.gerrit.plugins.replication.pull.ProjectLockFailureException;
import com.gerritforge.gerrit.plugins.replication.pull.ShutdownState;
import com.google.common.util.concurrent.Futures;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.RefUpdatedEvent;
import com.google.gerrit.server.permissions.PermissionBackendException;
import com.google.gerrit.server.util.time.TimeUtil;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventsBrokerMessageConsumerTest {

  public static final String TOPIC_NAME = "stream-events";
  private static final String INSTANCE_ID = "instance-id";
  private static final int RETRY_INTERVAL = 30_000;
  private static final int RETRY_POLL_INTERVAL = 0;
  @Mock private StreamEventListener eventListener;
  @Mock DynamicItem<BrokerApi> eventsBrokerDynamicItem;
  @Mock BrokerApi eventsBroker;
  @Mock EventsBrokerConfiguration eventsBrokerConfiguration;
  @Mock MessageAcknowledgement<Event> acknowledgement;

  EventsBrokerMessageConsumer objectUnderTest;
  ShutdownState shutdownState;

  private static final MessageAcknowledgement<Event> NOOP_ACK = (ignore) -> {};

  @Before
  public void setup() {
    shutdownState = new ShutdownState();
    when(eventsBrokerConfiguration.getPartitionsForTopic(TOPIC_NAME)).thenReturn(List.of());
    objectUnderTest =
        new EventsBrokerMessageConsumer(
            eventsBrokerDynamicItem,
            eventListener,
            eventsBrokerConfiguration,
            shutdownState,
            TOPIC_NAME,
            null,
            INSTANCE_ID,
            RETRY_INTERVAL,
            RETRY_POLL_INTERVAL);
  }

  @Test
  public void shouldRethrowExceptionWhenFetchThrowsAuthException() throws Exception {
    startAutoAckConsumer();
    doThrow(PermissionBackendException.class).when(eventListener).fetchRefsForEventAsync(any());
    assertThrows(
        EventRejectedException.class,
        () -> objectUnderTest.accept(new RefUpdatedEvent(), MessageMetadata.EMPTY, NOOP_ACK));
  }

  @Test
  public void shouldRethrowExceptionWhenFetchThrowsPermissionBackendException() throws Exception {
    startAutoAckConsumer();
    doThrow(PermissionBackendException.class).when(eventListener).fetchRefsForEventAsync(any());
    assertThrows(
        EventRejectedException.class,
        () -> objectUnderTest.accept(new RefUpdatedEvent(), MessageMetadata.EMPTY, NOOP_ACK));
  }

  @Test
  public void shouldNotThrowExceptionWhenFetchSucceed() throws Exception {
    startAutoAckConsumer();
    doNothing().when(eventListener).fetchRefsForEventAsync(any());
    objectUnderTest.accept(new RefUpdatedEvent(), MessageMetadata.EMPTY, NOOP_ACK);
  }

  @Test
  public void shouldSubscribeToAllEventsPartition() {
    objectUnderTest = createAllEventsPartitionConsumer();
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);

    objectUnderTest.onBrokerApiStarted();

    verify(eventsBroker)
        .receiveAsyncWithPartition(TOPIC_NAME, ALL_EVENTS_PARTITION, "groupId", objectUnderTest);
  }

  @Test
  public void shouldSubscribeToWholeTopicWhenNoPartitionsAreConfigured() {
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);
    when(eventsBroker.isAutoAck()).thenReturn(true);

    objectUnderTest.onBrokerApiStarted();

    verify(eventsBroker).receiveAsync(TOPIC_NAME, objectUnderTest);
  }

  @Test
  public void shouldFailToStartManualAckConsumerWhenAllEventsPartitionIsNotConfigured() {
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);
    when(eventsBroker.isAutoAck()).thenReturn(false);

    assertThrows(IllegalStateException.class, objectUnderTest::onBrokerApiStarted);

    verify(eventsBroker, never()).receiveAsync(TOPIC_NAME, objectUnderTest);
  }

  @Test
  public void shouldNotAcknowledgeWhenSynchronousFetchThrows() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    doThrow(PermissionBackendException.class).when(eventListener).fetchRefsForEventSync(any());
    Event event = new RefUpdatedEvent();

    assertThrows(
        EventRejectedException.class,
        () -> objectUnderTest.accept(event, MessageMetadata.EMPTY, acknowledgement));

    verify(eventsBroker, never()).requeue(any(), any(), any());
    verify(acknowledgement, never()).ack(event);
  }

  @Test
  public void shouldRequeueTransientFailureWithNextRetryMetadataAndAcknowledge() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);
    doThrow(IOException.class).when(eventListener).fetchRefsForEventSync(any());
    Event event = new RefUpdatedEvent();
    MessageMetadata metadata =
        new MessageMetadata(new MessageMetadata.Retry(2, 1234L, "previous-instance"));
    when(eventsBroker.requeue(any(), any(), any())).thenReturn(Futures.immediateFuture(true));

    objectUnderTest.accept(event, metadata, acknowledgement);

    ArgumentCaptor<MessageMetadata> metadataCaptor = ArgumentCaptor.forClass(MessageMetadata.class);
    InOrder inOrder = inOrder(eventsBroker, acknowledgement);
    inOrder.verify(eventsBroker).requeue(eq(TOPIC_NAME), eq(event), metadataCaptor.capture());
    inOrder.verify(acknowledgement).ack(event);
    MessageMetadata requeuedMetadata = metadataCaptor.getValue();
    assertThat(requeuedMetadata.retry().retryCount()).isEqualTo(3);
    assertThat(requeuedMetadata.retry().retryingInstanceId()).isEqualTo(INSTANCE_ID);
    assertThat(requeuedMetadata.retry().lastAttemptOn()).isGreaterThan(1234L);
  }

  @Test
  public void shouldRequeueThrottledRetryWithoutProcessingOrAdvancingMetadata() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);
    Event event = new RefUpdatedEvent();
    MessageMetadata metadata =
        new MessageMetadata(new MessageMetadata.Retry(2, TimeUtil.nowMs(), "previous-instance"));
    when(eventsBroker.requeue(TOPIC_NAME, event, metadata))
        .thenReturn(Futures.immediateFuture(true));

    objectUnderTest.accept(event, metadata, acknowledgement);

    verify(eventListener, never()).fetchRefsForEventSync(any());
    InOrder inOrder = inOrder(eventsBroker, acknowledgement);
    inOrder.verify(eventsBroker).requeue(eq(TOPIC_NAME), same(event), same(metadata));
    inOrder.verify(acknowledgement).ack(event);
  }

  @Test
  public void shouldProcessRetryAfterRetryIntervalElapses() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    Event event = new RefUpdatedEvent();
    MessageMetadata metadata =
        new MessageMetadata(
            new MessageMetadata.Retry(2, TimeUtil.nowMs() - RETRY_INTERVAL, "previous-instance"));

    objectUnderTest.accept(event, metadata, acknowledgement);

    InOrder inOrder = inOrder(eventListener, acknowledgement);
    inOrder.verify(eventListener).fetchRefsForEventSync(event);
    inOrder.verify(acknowledgement).ack(event);
    verify(eventsBroker, never()).requeue(any(), any(), any());
  }

  @Test
  public void shouldNotAcknowledgeWhenRequeueFails() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);
    doThrow(
            new ProjectLockFailureException(
                Project.nameKey("project"), "conflicting-task", new Exception("lock failure")))
        .when(eventListener)
        .fetchRefsForEventSync(any());
    Event event = new RefUpdatedEvent();
    when(eventsBroker.requeue(any(), any(), any())).thenReturn(Futures.immediateFuture(false));

    assertThrows(
        EventRejectedException.class,
        () -> objectUnderTest.accept(event, MessageMetadata.EMPTY, acknowledgement));

    verify(eventsBroker).requeue(any(), any(), any());
    verify(acknowledgement, never()).ack(event);
  }

  @Test
  public void shouldPropagateInterruptionWithoutRequeueingOrAcknowledging() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    doThrow(InterruptedException.class).when(eventListener).fetchRefsForEventSync(any());
    Event event = new RefUpdatedEvent();

    assertThrows(
        InterruptedException.class,
        () -> objectUnderTest.accept(event, MessageMetadata.EMPTY, acknowledgement));

    verify(eventsBroker, never()).requeue(any(), any(), any());
    verify(acknowledgement, never()).ack(event);
  }

  @Test
  public void shouldAcknowledgeAfterSynchronousFetchSucceeds() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    Event event = new RefUpdatedEvent();

    objectUnderTest.accept(event, MessageMetadata.EMPTY, acknowledgement);

    InOrder inOrder = inOrder(eventListener, acknowledgement);
    inOrder.verify(eventListener).fetchRefsForEventSync(event);
    inOrder.verify(acknowledgement).ack(event);
  }

  @Test
  public void shouldStillAcceptLastEventDuringShutdownAndThenDisconnect() throws Exception {
    startAutoAckConsumer();
    doNothing().when(eventListener).fetchRefsForEventAsync(any());

    shutdownState.setIsShuttingDown(true);

    objectUnderTest.accept(new RefUpdatedEvent(), MessageMetadata.EMPTY, NOOP_ACK);
    verify(eventsBroker, times(1)).disconnect(TOPIC_NAME, null);
  }

  private EventsBrokerMessageConsumer createAllEventsPartitionConsumer() {
    when(eventsBrokerConfiguration.getPartitionsForTopic(TOPIC_NAME))
        .thenReturn(List.of(ALL_EVENTS_PARTITION));
    return new EventsBrokerMessageConsumer(
        eventsBrokerDynamicItem,
        eventListener,
        eventsBrokerConfiguration,
        shutdownState,
        TOPIC_NAME,
        "groupId",
        INSTANCE_ID,
        RETRY_INTERVAL,
        RETRY_POLL_INTERVAL);
  }

  private void startAutoAckConsumer() {
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);
    when(eventsBroker.isAutoAck()).thenReturn(true);
    objectUnderTest.onBrokerApiStarted();
  }
}
