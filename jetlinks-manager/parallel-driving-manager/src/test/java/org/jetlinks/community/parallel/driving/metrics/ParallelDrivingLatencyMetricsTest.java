package org.jetlinks.community.parallel.driving.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ParallelDrivingLatencyMetricsTest {

    @Test
    void returnsTheOriginalPublisherWhenMetricsAreUnavailable() {
        ParallelDrivingLatencyMetrics metrics = new ParallelDrivingLatencyMetrics(null);
        Mono<String> publisher = Mono.just("ok");

        assertSame(publisher, metrics.observeControlOperation("control", publisher));
        StepVerifier.create(publisher)
            .expectNext("ok")
            .verifyComplete();
    }

    @Test
    void preservesSuccessAndErrorSignals() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ParallelDrivingLatencyMetrics metrics = new ParallelDrivingLatencyMetrics(registry);

        StepVerifier.create(metrics.observeControlOperation("control", Mono.just("ok")))
            .expectNext("ok")
            .verifyComplete();
        StepVerifier.create(metrics.observeControlOperation("control", Mono.error(new IllegalStateException("business failure"))))
            .expectErrorMessage("business failure")
            .verify();

        assertEquals(
            1,
            registry.get(ParallelDrivingLatencyMetrics.METRIC_CONTROL_REQUESTS)
                .tag("operation", "control")
                .tag("result", "success")
                .counter()
                .count()
        );
        assertEquals(
            1,
            registry.get(ParallelDrivingLatencyMetrics.METRIC_CONTROL_REQUESTS)
                .tag("operation", "control")
                .tag("result", "failure")
                .counter()
                .count()
        );
    }

    @Test
    void preservesCancellationWhenMetricRegistrationFails() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.counter(
            ParallelDrivingLatencyMetrics.METRIC_CONTROL_DURATION,
            "operation", "control",
            "result", "cancel"
        );
        ParallelDrivingLatencyMetrics metrics = new ParallelDrivingLatencyMetrics(registry);

        StepVerifier.create(metrics.observeControlOperation("control", Mono.never()))
            .thenCancel()
            .verify();
    }

    @Test
    void keepsStatusLatencyMetricNamesAndTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ParallelDrivingLatencyMetrics metrics = new ParallelDrivingLatencyMetrics(registry);

        metrics.recordStatusQueued("vehicle", 100, 110, 10);
        metrics.recordStatusQueued("cockpit", 100, 125, 25);

        assertEquals(
            1,
            registry.get("parallel_driving.status_websocket.eventbus_to_queue_latency")
                .tag("source", "vehicle")
                .timer()
                .count()
        );
        assertEquals(
            1,
            registry.get("parallel_driving.status_websocket.eventbus_to_queue_latency")
                .tag("source", "cockpit")
                .timer()
                .count()
        );
    }

    @Test
    void boundsControlMetricLabelsToKnownValues() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ParallelDrivingLatencyMetrics metrics = new ParallelDrivingLatencyMetrics(registry);

        StepVerifier.create(metrics.observeControlStage(
                "unexpected-operation",
                "device-" + "id",
                Mono.just("ok")
            ))
            .expectNext("ok")
            .verifyComplete();

        assertEquals(
            1,
            registry.get(ParallelDrivingLatencyMetrics.METRIC_CONTROL_STAGE_DURATION)
                .tag("operation", "unknown")
                .tag("stage", "unknown")
                .tag("result", "success")
                .timer()
                .count()
        );
    }
}
