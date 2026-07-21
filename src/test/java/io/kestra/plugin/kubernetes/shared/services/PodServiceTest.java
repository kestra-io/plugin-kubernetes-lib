package io.kestra.plugin.kubernetes.shared.services;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkingDir;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PodServiceTest {
    @Test
    void waitForContainersStartedOrCompletedShouldReturnImmediatelyWhenPodAlreadySucceeded() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-succeeded")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Succeeded")
            .endStatus()
            .build();

        when(podResource.get()).thenReturn(pod);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(30));

            assertThat(result, is(pod));
            // Fast path: no watch call should have been made
            verify(podResource, never()).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldReturnImmediatelyWhenPodAlreadyRunning() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-running")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .addNewContainerStatus()
            .withName("main")
            .withNewState().withNewRunning().endRunning().endState()
            .endContainerStatus()
            .endStatus()
            .build();

        when(podResource.get()).thenReturn(pod);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(30));

            assertThat(result, is(pod));
            // Fast path: no watch call should have been made
            verify(podResource, never()).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldReturnOnFallbackGetAfterTimeoutWhenConditionSatisfied() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pendingPod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-pending")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();
        var succeededPod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-pending")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Succeeded")
            .endStatus()
            .build();

        // Fast-path GET returns pending (no early return), watch times out, fallback GET returns succeeded
        when(podResource.get()).thenReturn(pendingPod, succeededPod);
        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS)))
            .thenThrow(new KubernetesClientTimeoutException("Timed out", "Pod", "pod-pending", 10L, TimeUnit.SECONDS));

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pendingPod)).thenReturn(podResource);

            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pendingPod, Duration.ofMinutes(10));

            assertThat(result, is(succeededPod));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldRetryOnClientTimeoutWhenPodStillExists() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .build();

        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS)))
            .thenThrow(new KubernetesClientTimeoutException("Timed out", "Pod", "pod-1", 10L, TimeUnit.SECONDS))
            .thenReturn(pod);
        when(podResource.get()).thenReturn(pod);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            // Budget > chunk (5 min) so the loop has budget remaining after the first timeout to retry
            var result = PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(10));

            assertThat(result, is(pod));
            // Two GET calls: one for the initial fast-path check, one after the chunk timeout
            Mockito.verify(podResource, Mockito.times(2)).get();
            Mockito.verify(podResource, Mockito.times(2)).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldThrowWhenPodAlreadyDeletedOnFastPath() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .build();

        // Fast-path GET returns null immediately — pod is already gone before watch is set up
        when(podResource.get()).thenReturn(null);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofMinutes(10))
            );

            assertThat(exception.getMessage(), is("Pod was deleted while waiting for containers to start: pod-1"));
            // Only the fast-path GET should have been called; watch is never reached
            Mockito.verify(podResource, Mockito.times(1)).get();
            verify(podResource, never()).waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS));
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldThrowWhenPodDeletedAfterWatch() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pendingPod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        // Fast-path GET returns a non-started pod (watch is entered), watch chunk times out, fallback GET returns null
        when(podResource.get()).thenReturn(pendingPod, (Pod) null);
        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS)))
            .thenThrow(new KubernetesClientTimeoutException("Timed out", "Pod", "pod-1", 10L, TimeUnit.SECONDS));

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pendingPod)).thenReturn(podResource);

            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForContainersStartedOrCompleted(client, logger, pendingPod, Duration.ofMinutes(10))
            );

            assertThat(exception.getMessage(), is("Pod was deleted while waiting for containers to start: pod-1"));
            // Two GET calls: fast-path (returns pending) + fallback after watch timeout (returns null)
            Mockito.verify(podResource, Mockito.times(2)).get();
        }
    }

    @Test
    void waitForCompletionShouldThrowWhenPodIsDeletedAfterWaitReturnsNull() {
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pod-1")
            .withNamespace("default")
            .endMetadata()
            .build();

        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS))).thenReturn(null);
        when(podResource.get()).thenReturn(null);

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, pod)).thenReturn(podResource);

            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForCompletion(client, logger, pod, Duration.ofMinutes(10))
            );

            assertThat(exception.getMessage(), is("Pod was deleted before reaching a terminal phase: pod-1"));
            Mockito.verify(podResource).get();
        }
    }

    @Test
    void withRetriesShouldWrapRetryFailureAsIoException() {
        var logger = mock(Logger.class);

        var exception = assertThrows(
            IOException.class, () -> PodService.withRetries(
                logger,
                "uploadMarker",
                () ->
                {
                    throw new KubernetesClientException("boom");
                }
            )
        );

        assertThat(exception.getMessage(), is("Failed to call 'uploadMarker'"));
    }

    @Test
    void shouldNotReturnPodWhenContainersReadyFalse() {
        KubernetesClient client = mock(KubernetesClient.class);

        Pod pod = new PodBuilder()
            .withNewMetadata()
            .withName("test-pod")
            .withNamespace("default")
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("test-container")
            .withImage("busybox")
            .endContainer()
            .endSpec()
            .withNewStatus()
            .withPhase("Running")
            .addNewCondition()
            .withType("ContainersReady")
            .withStatus("False")
            .endCondition()
            .endStatus()
            .build();

        @SuppressWarnings("rawtypes")
        MixedOperation pods = mock(MixedOperation.class);

        @SuppressWarnings("rawtypes")
        NonNamespaceOperation ns = mock(NonNamespaceOperation.class);

        PodResource podResource = mock(PodResource.class);

        when(client.pods()).thenReturn(pods);
        when(pods.inNamespace("default")).thenReturn(ns);
        when(ns.withName("test-pod")).thenReturn(podResource);

        when(podResource.waitUntilCondition(any(), anyLong(), any()))
            .thenAnswer(invocation ->
            {
                Predicate<Pod> predicate = invocation.getArgument(0);
                return predicate.test(pod) ? pod : null;
            });

        Pod result = PodService.waitForPodReady(client, pod, Duration.ofSeconds(1));

        assertNull(result);
    }

    @Test
    void waitForCompletionExceptShouldNotMatchWhenAllContainersAreExcluded() {
        // When filtering out the only container, the stream is empty.
        // allMatch on an empty stream returns true (vacuous truth), which is a bug:
        // it would cause the predicate to return true prematurely.
        var pod = new PodBuilder()
            .withNewMetadata().withName("pod-1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .addNewContainerStatus()
            .withName("sidecar")
            .withNewState().withNewRunning().endRunning().endState()
            .endContainerStatus()
            .endStatus()
            .build();

        // Extract the predicate logic inline to test it directly
        var status = pod.getStatus();
        var except = "sidecar";

        // After filtering out "sidecar", the stream is empty.
        // The predicate must return false in this case.
        var result = status != null &&
            status.getContainerStatuses() != null &&
            status.getContainerStatuses()
                .stream()
                .anyMatch(cs -> !cs.getName().equals(except))
            &&
            status.getContainerStatuses()
                .stream()
                .filter(cs -> !cs.getName().equals(except))
                .allMatch(cs -> cs.getState() != null && cs.getState().getTerminated() != null);

        assertFalse(result, "Predicate must return false when all containers are excluded (empty filtered stream)");
    }

    @Test
    void waitForCompletionExceptShouldNotMatchWhenContainerStatusesAreNull() {
        var pod = new PodBuilder()
            .withNewMetadata().withName("pod-1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        // Simulate null containerStatuses (common during early pod lifecycle)
        pod.getStatus().setContainerStatuses(null);

        var status = pod.getStatus();
        var except = "sidecar";

        var result = status != null &&
            status.getContainerStatuses() != null &&
            status.getContainerStatuses()
                .stream()
                .anyMatch(cs -> !cs.getName().equals(except))
            &&
            status.getContainerStatuses()
                .stream()
                .filter(cs -> !cs.getName().equals(except))
                .allMatch(cs -> cs.getState() != null && cs.getState().getTerminated() != null);

        assertFalse(result, "Predicate must return false when containerStatuses is null");
    }

    @Test
    void waitForCompletionExceptShouldMatchWhenNonExcludedContainersAreTerminated() {
        var pod = new PodBuilder()
            .withNewMetadata().withName("pod-1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .addNewContainerStatus()
            .withName("main")
            .withNewState().withNewTerminated().withExitCode(0).endTerminated().endState()
            .endContainerStatus()
            .addNewContainerStatus()
            .withName("sidecar")
            .withNewState().withNewRunning().endRunning().endState()
            .endContainerStatus()
            .endStatus()
            .build();

        var status = pod.getStatus();
        var except = "sidecar";

        var result = status != null &&
            status.getContainerStatuses() != null &&
            status.getContainerStatuses()
                .stream()
                .anyMatch(cs -> !cs.getName().equals(except))
            &&
            status.getContainerStatuses()
                .stream()
                .filter(cs -> !cs.getName().equals(except))
                .allMatch(cs -> cs.getState() != null && cs.getState().getTerminated() != null);

        assertThat("Predicate must return true when non-excluded containers are terminated", result, is(true));
    }

    @Test
    void waitForCompletionExceptMustNotWaitForExcludedContainerToReachTerminalPodPhase() {
        // The excluded container (the output-files sidecar) is deliberately kept running past this call so it can
        // still serve file downloads; it is only signaled to exit afterwards. The pod's overall phase therefore
        // stays "Running" for that whole window, so requiring a terminal phase here would deadlock forever.
        var client = mock(KubernetesClient.class);
        var logger = mock(Logger.class);
        var podResource = mock(PodResource.class);

        var mainTerminatedSidecarStillRunning = new PodBuilder()
            .withNewMetadata().withName("pod-1").withNamespace("default").endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .addNewContainerStatus()
            .withName("main")
            .withNewState().withNewTerminated().withExitCode(0).endTerminated().endState()
            .endContainerStatus()
            .addNewContainerStatus()
            .withName("sidecar")
            .withNewState().withNewRunning().endRunning().endState()
            .endContainerStatus()
            .endStatus()
            .build();

        when(podResource.waitUntilCondition(any(), anyLong(), Mockito.eq(TimeUnit.SECONDS)))
            .thenAnswer(invocation ->
            {
                Predicate<Pod> condition = invocation.getArgument(0);
                return condition.test(mainTerminatedSidecarStillRunning) ? mainTerminatedSidecarStillRunning : null;
            });

        try (var podService = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            podService.when(() -> PodService.podRef(client, mainTerminatedSidecarStillRunning)).thenReturn(podResource);

            var result = PodService.waitForCompletionExcept(client, logger, mainTerminatedSidecarStillRunning, Duration.ofMinutes(10), "sidecar");

            assertThat("waitForCompletionExcept must return once non-excluded containers terminate, without waiting for the pod phase", result, is(mainTerminatedSidecarStillRunning));
        }
    }

    @Test
    void uploadMarkerShouldOverwriteStaleMarkerFileLeftBehindByAPreviousRun(@TempDir Path tempDir) throws Exception {
        // PodService.tempDir(runContext) is reused across multiple run() calls sharing the same
        // RunContext/working directory (e.g. KubernetesAdditionalSpecTest#canWorkMultipleTimeInSameWdir).
        // If a previous run's local marker file failed to get deleted after upload, the next run's
        // createNewFile() call must not fail - the marker is disposable and safe to overwrite.
        var workingDir = tempDir.resolve("working-dir");
        Files.createDirectory(workingDir);
        var staleMarkerFile = workingDir.resolve("ready");
        Files.writeString(staleMarkerFile, "stale content left behind by a previous run");

        var runContext = mock(RunContext.class);
        var workingDirService = mock(WorkingDir.class);
        when(runContext.workingDir()).thenReturn(workingDirService);
        when(workingDirService.path()).thenReturn(tempDir);

        var podResource = mock(PodResource.class, Mockito.RETURNS_DEEP_STUBS);
        when(podResource.inContainer(anyString()).withReadyWaitTimeout(anyInt()).file(anyString()).upload(any(Path.class)))
            .thenReturn(true);

        var logger = mock(Logger.class);

        PodService.uploadMarker(runContext, podResource, logger, "ready", "container");

        assertFalse(Files.exists(staleMarkerFile), "the stale marker file must be cleaned up so the next run can create it again");
    }
}
