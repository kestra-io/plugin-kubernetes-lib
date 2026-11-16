package io.kestra.plugin.kubernetes.shared.services;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for kestra-io/plugin-kubernetes#237.
 *
 * <p>{@code close()} used to return while a scheduled tick was still inside a fabric8 log call.
 * The tick could then open a {@link LogWatch} after the watches had been drained, so nothing ever
 * closed that watch: it kept the {@link KubernetesClient} reachable after {@code PodCreate}'s
 * try-with-resources had closed the client, and the underlying request (plus its HTTP-client
 * threads) outlived the task.
 */
class PodLogServiceCloseRaceTest {

    private record Fixture(KubernetesClient client, Pod pod, RunContext runContext,
                           AbstractLogConsumer consumer) {
    }

    private static Pod pod() {
        Pod pod = mock(Pod.class);
        PodSpec spec = mock(PodSpec.class);
        Container container = mock(Container.class);
        when(container.getName()).thenReturn("main");
        when(pod.getSpec()).thenReturn(spec);
        when(spec.getContainers()).thenReturn(List.of(container));
        when(pod.getMetadata()).thenReturn(
            new ObjectMetaBuilder().withNamespace("default").withName("test-pod").build()
        );
        return pod;
    }

    private static RunContext runContext() {
        RunContext runContext = mock(RunContext.class);
        when(runContext.logger()).thenReturn(mock(Logger.class));
        return runContext;
    }

    private static Fixture fixture(TimestampBytesLimitTerminateTimeTailPrettyLoggable logBuilder) {
        KubernetesClient client = mock(KubernetesClient.class);
        @SuppressWarnings("unchecked")
        MixedOperation<Pod, PodList, PodResource> podsOp = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Pod, PodList, PodResource> nsOp = mock(NonNamespaceOperation.class);
        PodResource podResource = mock(PodResource.class);
        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace("default")).thenReturn(nsOp);
        when(nsOp.withName("test-pod")).thenReturn(podResource);

        ContainerResource containerResource = mock(ContainerResource.class);
        when(podResource.inContainer("main")).thenReturn(containerResource);
        when(containerResource.withReadyWaitTimeout(0)).thenReturn(containerResource);
        when(containerResource.usingTimestamps()).thenReturn(logBuilder);
        when(logBuilder.sinceTime(any())).thenReturn(logBuilder);

        return new Fixture(client, pod(), runContext(), mock(AbstractLogConsumer.class));
    }

    /**
     * A client whose log call blocks until {@code release} is counted down, emulating a fabric8
     * log read that does not react to interruption.
     */
    private static Fixture blockedFixture(CountDownLatch entered, CountDownLatch release, List<LogWatch> watches) {
        TimestampBytesLimitTerminateTimeTailPrettyLoggable logBuilder =
            mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        when(logBuilder.watchLog(any())).thenAnswer(invocation ->
        {
            entered.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // swallowed on purpose: the real blocking read ignores interrupts too
                }
            }
            LogWatch watch = mock(LogWatch.class);
            watches.add(watch);
            return watch;
        });
        return fixture(logBuilder);
    }

    private static PodLogService start(Fixture fixture) {
        PodLogService service = new PodLogService();
        service.setRefreshInterval(3600);
        service.watch(fixture.client(), fixture.pod(), fixture.consumer(), fixture.runContext(), "main");
        return service;
    }

    private static long liveLogThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.isAlive() && t.getName().startsWith("k8s-log"))
            .count();
    }

    @Test
    void closeWaitsForTheInFlightTick() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<LogWatch> watches = new CopyOnWriteArrayList<>();
        PodLogService service = start(blockedFixture(entered, release, watches));

        assertTrue(entered.await(5, TimeUnit.SECONDS), "scheduled tick never reached watchLog()");

        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            Future<?> closing = closer.submit(() ->
            {
                service.close();
                return null;
            });

            Thread.sleep(300);
            assertFalse(closing.isDone(), "close() returned while a tick was still inside a log call");

            release.countDown();
            closing.get(5, TimeUnit.SECONDS);

            assertTrue(liveLogThreads() == 0, "a k8s-log executor thread survived close()");
        } finally {
            release.countDown();
            closer.shutdownNow();
        }
    }

    @Test
    void watchCreatedByAnInFlightTickIsClosed() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<LogWatch> watches = new CopyOnWriteArrayList<>();
        PodLogService service = start(blockedFixture(entered, release, watches));

        assertTrue(entered.await(5, TimeUnit.SECONDS), "scheduled tick never reached watchLog()");

        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            Future<?> closing = closer.submit(() ->
            {
                service.close();
                return null;
            });
            Thread.sleep(200);
            release.countDown();
            closing.get(5, TimeUnit.SECONDS);

            assertFalse(watches.isEmpty(), "expected the in-flight tick to create one log watch");
            verify(watches.get(0)).close();
        } finally {
            release.countDown();
            closer.shutdownNow();
        }
    }

    @Test
    void repeatedOpenCloseLeavesNoLogThreadBehind() throws Exception {
        int cycles = 200;
        AtomicInteger watchCalls = new AtomicInteger();
        List<LogWatch> watches = new CopyOnWriteArrayList<>();

        TimestampBytesLimitTerminateTimeTailPrettyLoggable logBuilder =
            mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        when(logBuilder.watchLog(any())).thenAnswer(invocation ->
        {
            watchCalls.incrementAndGet();
            LogWatch watch = mock(LogWatch.class);
            watches.add(watch);
            return watch;
        });
        Fixture fixture = fixture(logBuilder);

        long baseline = liveLogThreads();
        for (int i = 0; i < cycles; i++) {
            PodLogService service = start(fixture);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (watchCalls.get() < i + 1 && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            service.close();
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (liveLogThreads() > baseline && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }

        assertTrue(
            liveLogThreads() <= baseline,
            "log threads accumulated across " + cycles + " open/close cycles: " + liveLogThreads()
        );
        long unclosed = watches.stream()
            .filter(watch -> mockingDetails(watch).getInvocations().isEmpty())
            .count();
        assertTrue(unclosed == 0, unclosed + " log watches were never closed");
    }

    @Test
    void closeIsIdempotent() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<LogWatch> watches = new CopyOnWriteArrayList<>();
        PodLogService service = start(blockedFixture(entered, release, watches));

        assertTrue(entered.await(5, TimeUnit.SECONDS));
        release.countDown();
        Thread.sleep(100);

        assertDoesNotThrow(() ->
        {
            service.close();
            service.close();
        });
    }
}
