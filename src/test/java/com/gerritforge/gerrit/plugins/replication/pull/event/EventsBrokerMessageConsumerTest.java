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
import static com.google.gerrit.testing.GerritJUnit.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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
import com.gerritforge.gerrit.plugins.replication.pull.ShutdownState;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.RefUpdatedEvent;
import com.google.gerrit.server.permissions.PermissionBackendException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventsBrokerMessageConsumerTest {

  public static final String TOPIC_NAME = "stream-events";
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
            null);
  }

  @Test
  public void shouldRethrowExceptionWhenFetchThrowsAuthException() throws Exception {
    doThrow(PermissionBackendException.class).when(eventListener).fetchRefsForEventAsync(any());
    assertThrows(
        EventRejectedException.class,
        () -> objectUnderTest.accept(new RefUpdatedEvent(), NOOP_ACK));
  }

  @Test
  public void shouldRethrowExceptionWhenFetchThrowsPermissionBackendException() throws Exception {
    doThrow(PermissionBackendException.class).when(eventListener).fetchRefsForEventAsync(any());
    assertThrows(
        EventRejectedException.class,
        () -> objectUnderTest.accept(new RefUpdatedEvent(), NOOP_ACK));
  }

  @Test
  public void shouldNotThrowExceptionWhenFetchSucceed() throws Exception {
    doNothing().when(eventListener).fetchRefsForEventAsync(any());
    objectUnderTest.accept(new RefUpdatedEvent(), NOOP_ACK);
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

    objectUnderTest.onBrokerApiStarted();

    verify(eventsBroker).receiveAsync(TOPIC_NAME, objectUnderTest);
  }

  @Test
  public void shouldNotAcknowledgeWhenSynchronousFetchThrows() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    doThrow(PermissionBackendException.class).when(eventListener).fetchRefsForEventSync(any());
    Event event = new RefUpdatedEvent();

    assertThrows(
        EventRejectedException.class, () -> objectUnderTest.accept(event, acknowledgement));

    verify(acknowledgement, never()).ack(event);
  }

  @Test
  public void shouldAcknowledgeAfterSynchronousFetchSucceeds() throws Exception {
    objectUnderTest = createAllEventsPartitionConsumer();
    Event event = new RefUpdatedEvent();

    objectUnderTest.accept(event, acknowledgement);

    InOrder inOrder = inOrder(eventListener, acknowledgement);
    inOrder.verify(eventListener).fetchRefsForEventSync(event);
    inOrder.verify(acknowledgement).ack(event);
  }

  @Test
  public void shouldStillAcceptLastEventDuringShutdownAndThenDisconnect() throws Exception {
    doNothing().when(eventListener).fetchRefsForEventAsync(any());
    when(eventsBrokerDynamicItem.get()).thenReturn(eventsBroker);

    shutdownState.setIsShuttingDown(true);

    objectUnderTest.accept(new RefUpdatedEvent(), NOOP_ACK);
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
        "groupId");
  }
}
