package io.kestra.plugin.kubernetes.shared.services;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.TimestampBytesLimitTerminateTimeTailPrettyLoggable;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
public class PodLogServiceTest {
    @Test
    void reconnectsAfterThreeHours() throws Exception {
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        when(logBuilder.watchLog(any())).thenReturn(mock(LogWatch.class));
        var call = logCall(logBuilder);

        var clock = mock(Clock.class);
        var startTime = Instant.parse("2023-01-01T00:00:00Z");
        when(clock.instant()).thenReturn(startTime);
        when(clock.getZone()).thenReturn(java.time.ZoneId.of("UTC"));

        var svc = new PodLogService();
        svc.setClock(clock);
        svc.setRefreshInterval(1);
        svc.watch(call.client(), call.pod(), call.consumer(), call.runContext(), "main");

        // Wait for initial connection (immediate execution)
        Thread.sleep(200);
        Mockito.verify(logBuilder, Mockito.times(1)).watchLog(any());

        // Advance time by 3 hours + 1 second
        when(clock.instant()).thenReturn(startTime.plus(Duration.ofHours(3).plusSeconds(1)));

        // Wait for next scheduled run (approx 1 second later)
        Thread.sleep(1200);

        // --- Assert ---
        // Should have called watchLog 2 times now (initial + reconnect)
        Mockito.verify(logBuilder, Mockito.atLeast(2)).watchLog(any());

        svc.close();
    }

    @Test
    void doesNotSpamReconnectTraceWhenNoNewLogsArrive() throws Exception {
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        when(logBuilder.watchLog(any())).thenReturn(mock(LogWatch.class));
        var call = logCall(logBuilder);
        var logger = call.runContext().logger();

        var svc = startWatching(call, 1);

        Thread.sleep(2400);

        Mockito.verify(logBuilder, Mockito.atLeast(2)).watchLog(any());
        Mockito.verify(logger, Mockito.times(1)).trace("No log since '{}', reconnecting", "unknown");

        svc.close();
    }

    @Test
    void closeDoesNotLogCancellationNoise() throws Exception {
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        when(logBuilder.watchLog(any())).thenReturn(mock(LogWatch.class));
        var call = logCall(logBuilder);
        var logger = call.runContext().logger();

        var svc = startWatching(call, 1);
        Thread.sleep(200);
        svc.close();
        Thread.sleep(200);

        Mockito.verify(logger, Mockito.never()).debug(eq("{} cancelled"), anyString(), any(CancellationException.class));
        Mockito.verify(logger, Mockito.never()).debug(eq("{} interrupted"), anyString(), any(InterruptedException.class));
        Mockito.verify(logger, Mockito.never()).error(eq("{} exception"), anyString(), any(Throwable.class));
    }

    @Test
    void fetchFinalLogsWarnsWhenNoLogConsumerIsSet() {
        var client = mock(KubernetesClient.class);
        var pod = mock(Pod.class);

        when(pod.getMetadata()).thenReturn(
            new ObjectMetaBuilder().withNamespace("default").withName("test-pod").build()
        );

        RunContext runContext = mock(RunContext.class);
        Logger logger = mock(Logger.class);
        when(runContext.logger()).thenReturn(logger);

        // no setLogConsumer() / watch() call: this reproduces the ordering bug where
        // fetchFinalLogs() ran before the consumer was wired up
        PodLogService svc = new PodLogService();

        assertDoesNotThrow(() -> svc.fetchFinalLogs(client, pod, runContext));

        Mockito.verify(logger).warn(contains("no log consumer was set"), eq("test-pod"));
        // it must stay a dead end, just a loud one: no API call is attempted
        Mockito.verifyNoInteractions(client);
    }

    // Regression tests for kestra-io/plugin-kubernetes#237.

    private record LogCall(KubernetesClient client, Pod pod, RunContext runContext,
                           AbstractLogConsumer consumer) {
    }

    private static LogCall logCall(TimestampBytesLimitTerminateTimeTailPrettyLoggable logBuilder) {
        var client = mock(KubernetesClient.class);
        var pod = mock(Pod.class);
        var spec = mock(PodSpec.class);
        var container = mock(Container.class);

        when(container.getName()).thenReturn("main");
        when(pod.getSpec()).thenReturn(spec);
        when(spec.getContainers()).thenReturn(List.of(container));
        when(pod.getMetadata()).thenReturn(
            new ObjectMetaBuilder().withNamespace("default").withName("test-pod").build()
        );

        @SuppressWarnings("unchecked")
        MixedOperation<Pod, PodList, PodResource> podsOp = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Pod, PodList, PodResource> nsOp = mock(NonNamespaceOperation.class);
        var podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace("default")).thenReturn(nsOp);
        when(nsOp.withName("test-pod")).thenReturn(podResource);
        when(podResource.get()).thenReturn(pod);

        var containerResource = mock(ContainerResource.class);
        when(podResource.inContainer("main")).thenReturn(containerResource);
        when(containerResource.withReadyWaitTimeout(0)).thenReturn(containerResource);
        when(containerResource.usingTimestamps()).thenReturn(logBuilder);
        when(logBuilder.sinceTime(any())).thenReturn(logBuilder);

        var runContext = mock(RunContext.class);
        when(runContext.logger()).thenReturn(mock(Logger.class));

        return new LogCall(client, pod, runContext, mock(AbstractLogConsumer.class));
    }

    // A log request that blocks until released, so the tick is mid-request when close() runs.
    private static LogCall blockedLogCall(CountDownLatch entered, CountDownLatch release, List<LogWatch> watches) {
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        when(logBuilder.watchLog(any())).thenAnswer(invocation ->
        {
            entered.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // the real request is not abandoned on interrupt either
                }
            }
            var watch = mock(LogWatch.class);
            watches.add(watch);
            return watch;
        });
        return logCall(logBuilder);
    }

    private static PodLogService startWatching(LogCall call, long refreshInterval) {
        var svc = new PodLogService();
        svc.setRefreshInterval(refreshInterval);
        svc.watch(call.client(), call.pod(), call.consumer(), call.runContext(), "main");
        return svc;
    }

    private static Thread closeInBackground(PodLogService svc) {
        var closer = new Thread(() ->
        {
            try {
                svc.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, "pod-log-closer");
        closer.start();
        return closer;
    }

    // Waits until close() is parked in awaitTermination(), so the tick takes the late-register branch.
    private static void awaitCloseProgress(Thread closer) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && closer.isAlive()) {
            if (Arrays.stream(closer.getStackTrace())
                .anyMatch(element -> "awaitTermination".equals(element.getMethodName()))) {
                return;
            }
            Thread.sleep(1);
        }
    }

    private static long liveLogThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.isAlive() && t.getName().startsWith("k8s-log"))
            .count();
    }

    // A pool can report terminated while its thread is still unwinding, so poll to a deadline.
    private static void awaitNoLogThreads(long baseline) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (liveLogThreads() > baseline && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertEquals(baseline, liveLogThreads(), "k8s-log threads survived close()");
    }

    @Test
    void closeWaitsForTheInFlightTick() throws Exception {
        var baseline = liveLogThreads();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var watches = new CopyOnWriteArrayList<LogWatch>();
        var svc = startWatching(blockedLogCall(entered, release, watches), 3600);

        assertTrue(entered.await(5, TimeUnit.SECONDS), "scheduled tick never reached watchLog()");

        var closer = closeInBackground(svc);
        try {
            awaitCloseProgress(closer);
            assertTrue(closer.isAlive(), "close() returned while the tick was still inside the log request");

            release.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(closer.isAlive(), "close() did not return once the tick finished");

            awaitNoLogThreads(baseline);
        } finally {
            release.countDown();
            closer.interrupt();
        }
    }

    private static void awaitWatch(List<LogWatch> watches) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (watches.isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
    }

    @Test
    void watchRegisteredWhileClosingIsClosed() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var watches = new CopyOnWriteArrayList<LogWatch>();
        var svc = startWatching(blockedLogCall(entered, release, watches), 3600);

        assertTrue(entered.await(5, TimeUnit.SECONDS), "scheduled tick never reached watchLog()");

        var closer = closeInBackground(svc);
        try {
            awaitCloseProgress(closer);
            release.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(10));
            awaitWatch(watches);

            assertFalse(watches.isEmpty(), "expected the in-flight tick to create one log watch");
            verify(watches.getFirst()).close();
        } finally {
            release.countDown();
            closer.interrupt();
        }
    }
}
