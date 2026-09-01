package io.kestra.plugin.kubernetes.shared.services;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CancellationException;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
public class PodLogServiceTest {
    @Test
    void reconnectsAfterThreeHours() throws Exception {
        // --- Arrange ---
        KubernetesClient client = mock(KubernetesClient.class);
        Pod pod = mock(Pod.class);
        PodSpec spec = mock(PodSpec.class);
        Container container = mock(Container.class);

        when(container.getName()).thenReturn("main");
        when(pod.getSpec()).thenReturn(spec);
        when(spec.getContainers()).thenReturn(List.of(container));
        when(pod.getMetadata()).thenReturn(
            new ObjectMetaBuilder().withNamespace("default").withName("test-pod").build()
        );

        // Mock Kubernetes client chain
        @SuppressWarnings("unchecked")
        MixedOperation<Pod, PodList, PodResource> podsOp = mock(MixedOperation.class);
        @SuppressWarnings("unchecked")
        NonNamespaceOperation<Pod, PodList, PodResource> nsOp = mock(NonNamespaceOperation.class);
        PodResource podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace("default")).thenReturn(nsOp);
        when(nsOp.withName("test-pod")).thenReturn(podResource);
        when(podResource.get()).thenReturn(pod);

        // Mock log watching
        var containerResource = mock(ContainerResource.class);
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        var logWatch = mock(LogWatch.class);

        when(podResource.inContainer("main")).thenReturn(containerResource);
        when(containerResource.usingTimestamps()).thenReturn(logBuilder);
        when(logBuilder.sinceTime(any())).thenReturn(logBuilder);
        when(logBuilder.watchLog(any())).thenReturn(logWatch);

        RunContext runContext = mock(RunContext.class);
        when(runContext.logger()).thenReturn(mock(Logger.class));
        AbstractLogConsumer logConsumer = mock(AbstractLogConsumer.class);

        // Mock Clock
        Clock clock = mock(Clock.class);
        Instant startTime = Instant.parse("2023-01-01T00:00:00Z");
        when(clock.instant()).thenReturn(startTime);
        when(clock.getZone()).thenReturn(java.time.ZoneId.of("UTC"));

        PodLogService svc = new PodLogService();
        svc.setClock(clock);
        svc.setRefreshInterval(1); // Set refresh interval to 1 second for faster testing

        // --- Act ---
        svc.watch(client, pod, logConsumer, runContext, container.getName());

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
        KubernetesClient client = mock(KubernetesClient.class);
        Pod pod = mock(Pod.class);
        PodSpec spec = mock(PodSpec.class);
        Container container = mock(Container.class);

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
        PodResource podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace("default")).thenReturn(nsOp);
        when(nsOp.withName("test-pod")).thenReturn(podResource);
        when(podResource.get()).thenReturn(pod);

        var containerResource = mock(ContainerResource.class);
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        var logWatch = mock(LogWatch.class);

        when(podResource.inContainer("main")).thenReturn(containerResource);
        when(containerResource.usingTimestamps()).thenReturn(logBuilder);
        when(logBuilder.sinceTime(any())).thenReturn(logBuilder);
        when(logBuilder.watchLog(any())).thenReturn(logWatch);

        RunContext runContext = mock(RunContext.class);
        Logger logger = mock(Logger.class);
        when(runContext.logger()).thenReturn(logger);
        AbstractLogConsumer logConsumer = mock(AbstractLogConsumer.class);

        PodLogService svc = new PodLogService();
        svc.setRefreshInterval(1);

        svc.watch(client, pod, logConsumer, runContext, container.getName());

        Thread.sleep(2400);

        Mockito.verify(logBuilder, Mockito.atLeast(2)).watchLog(any());
        Mockito.verify(logger, Mockito.times(1)).trace("No log since '{}', reconnecting", "unknown");

        svc.close();
    }

    @Test
    void closeDoesNotLogCancellationNoise() throws Exception {
        KubernetesClient client = mock(KubernetesClient.class);
        Pod pod = mock(Pod.class);
        PodSpec spec = mock(PodSpec.class);
        Container container = mock(Container.class);

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
        PodResource podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(podsOp);
        when(podsOp.inNamespace("default")).thenReturn(nsOp);
        when(nsOp.withName("test-pod")).thenReturn(podResource);
        when(podResource.get()).thenReturn(pod);

        var containerResource = mock(ContainerResource.class);
        var logBuilder = mock(TimestampBytesLimitTerminateTimeTailPrettyLoggable.class);
        var logWatch = mock(LogWatch.class);

        when(podResource.inContainer("main")).thenReturn(containerResource);
        when(containerResource.usingTimestamps()).thenReturn(logBuilder);
        when(logBuilder.sinceTime(any())).thenReturn(logBuilder);
        when(logBuilder.watchLog(any())).thenReturn(logWatch);

        RunContext runContext = mock(RunContext.class);
        Logger logger = mock(Logger.class);
        when(runContext.logger()).thenReturn(logger);
        AbstractLogConsumer logConsumer = mock(AbstractLogConsumer.class);

        PodLogService svc = new PodLogService();
        svc.setRefreshInterval(1);

        svc.watch(client, pod, logConsumer, runContext, container.getName());
        Thread.sleep(200);
        svc.close();
        Thread.sleep(200);

        Mockito.verify(logger, Mockito.never()).debug(eq("{} cancelled"), anyString(), any(CancellationException.class));
        Mockito.verify(logger, Mockito.never()).debug(eq("{} interrupted"), anyString(), any(InterruptedException.class));
        Mockito.verify(logger, Mockito.never()).error(eq("{} exception"), anyString(), any(Throwable.class));
    }

    @Test
    void fetchFinalLogsWarnsWhenNoLogConsumerIsSet() {
        KubernetesClient client = mock(KubernetesClient.class);
        Pod pod = mock(Pod.class);

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
}
