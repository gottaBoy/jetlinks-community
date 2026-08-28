package org.jetlinks.community.parallel.driving.room;

import org.jetlinks.community.parallel.driving.metrics.ParallelDrivingLatencyMetrics;
import org.jetlinks.core.device.DeviceMessageSender;
import org.jetlinks.core.device.DeviceOperator;
import org.jetlinks.core.message.DeviceMessage;
import org.jetlinks.core.message.function.FunctionInvokeMessage;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParallelDrivingRoomTest {

    @Test
    void latestOnlyKeepsOneInflightAndSendsOnlyNewestPendingFrame() {
        DeviceOperator cockpit = mock(DeviceOperator.class);
        DeviceOperator vehicle = mock(DeviceOperator.class);
        DeviceMessageSender sender = mock(DeviceMessageSender.class);
        ParallelDrivingLatencyMetrics metrics = mock(ParallelDrivingLatencyMetrics.class);
        Sinks.Empty<Void> firstSend = Sinks.empty();
        AtomicInteger sendCount = new AtomicInteger();
        List<String> sentMessageIds = new ArrayList<>();

        when(vehicle.messageSender()).thenReturn(sender);
        when(sender.sendAndForget(any(DeviceMessage.class))).thenAnswer(invocation -> {
            DeviceMessage message = invocation.getArgument(0);
            sentMessageIds.add(message.getMessageId());
            return sendCount.getAndIncrement() == 0 ? firstSend.asMono() : Mono.empty();
        });

        ParallelDrivingRoom room = new ParallelDrivingRoom("cockpit-1", "vehicle-1");
        room.initialize(cockpit, vehicle);
        room.setLatestOnlyEnabled(true);
        room.setLatencyMetrics(metrics);

        room.forwardCockpitToVehicle(remoteJoystick("message-1", "1")).subscribe();
        room.forwardCockpitToVehicle(remoteJoystick("message-2", "2")).block();
        room.forwardCockpitToVehicle(remoteJoystick("message-3", "3")).block();

        assertEquals(List.of("message-1"), sentMessageIds);
        assertEquals(1, room.getCoalescedRemoteJoystickMessages().get());

        firstSend.tryEmitEmpty();

        assertEquals(List.of("message-1", "message-3"), sentMessageIds);
        verify(metrics).recordRemoteJoystickMailboxCoalesced("cockpit-1", "vehicle-1");
        verify(metrics, times(2)).recordRemoteJoystickSendStarted("cockpit-1", "vehicle-1");
        verify(metrics, times(2)).remoteJoystickSendFinished();
        verify(metrics, times(2)).recordRemoteJoystickMailboxPendingAge(
            any(), any(), any(Long.class));
        verify(metrics, times(2)).recordRemoteJoystickSendCompletion(
            any(), any(), any(Long.class), any());
    }

    private static FunctionInvokeMessage remoteJoystick(String messageId, String sequence) {
        FunctionInvokeMessage message = new FunctionInvokeMessage();
        message.setDeviceId("cockpit-1");
        message.setFunctionId("remotejoystick");
        message.setMessageId(messageId);
        message.addInput("seq", sequence);
        message.addHeader("seq", sequence);
        message.addHeader("correlationId", messageId);
        return message;
    }
}
