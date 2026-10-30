package io.kestra.plugin.kubernetes.shared.services;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableKubernetesMockClient(crud = true)
class PodServiceInformerTest {
    private KubernetesClient client;

    @Test
    void waitForCompletionShouldShortCircuitWhenPodIsDeletedDuringWaitAgainstRealInformer() {
        var logger = LoggerFactory.getLogger(PodServiceInformerTest.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("running-pod-deleted")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .endStatus()
            .build();

        client.pods().inNamespace("default").resource(pod).create();

        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            executor.schedule(() -> {
                client.pods().inNamespace("default").withName("running-pod-deleted").withGracePeriod(0).delete();
            }, 1, TimeUnit.SECONDS);

            long start = System.currentTimeMillis();
            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForCompletion(client, logger, pod, Duration.ofSeconds(60))
            );
            long elapsed = System.currentTimeMillis() - start;

            assertThat(exception.getMessage(), is("Pod was deleted before reaching a terminal phase: running-pod-deleted"));
            assertTrue(elapsed < 10_000, "Expected wait to short-circuit in under 10s on deletion, but took " + elapsed + "ms");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void waitForContainersStartedOrCompletedShouldShortCircuitWhenPodIsDeletedDuringWaitAgainstRealInformer() {
        var logger = LoggerFactory.getLogger(PodServiceInformerTest.class);
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pending-pod-deleted")
            .withNamespace("default")
            .endMetadata()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        client.pods().inNamespace("default").resource(pod).create();

        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            executor.schedule(() -> {
                client.pods().inNamespace("default").withName("pending-pod-deleted").withGracePeriod(0).delete();
            }, 1, TimeUnit.SECONDS);

            long start = System.currentTimeMillis();
            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForContainersStartedOrCompleted(client, logger, pod, Duration.ofSeconds(60))
            );
            long elapsed = System.currentTimeMillis() - start;

            assertThat(exception.getMessage(), is("Pod was deleted while waiting for containers to start: pending-pod-deleted"));
            assertTrue(elapsed < 10_000, "Expected wait to short-circuit in under 10s on deletion, but took " + elapsed + "ms");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void waitForPodReadyShouldShortCircuitWhenPodIsDeletedDuringWaitAgainstRealInformer() {
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pending-pod-ready-deleted")
            .withNamespace("default")
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        client.pods().inNamespace("default").resource(pod).create();

        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            executor.schedule(() -> {
                client.pods().inNamespace("default").withName("pending-pod-ready-deleted").withGracePeriod(0).delete();
            }, 1, TimeUnit.SECONDS);

            long start = System.currentTimeMillis();
            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForPodReady(client, pod, Duration.ofSeconds(60))
            );
            long elapsed = System.currentTimeMillis() - start;

            assertThat(exception.getMessage(), is("Pod 'pending-pod-ready-deleted' no longer exists while waiting for it to be ready; it was deleted or evicted outside this task, check the namespace events"));
            assertTrue(elapsed < 10_000, "Expected wait to short-circuit in under 10s on deletion, but took " + elapsed + "ms");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void waitForInitContainerRunningShouldShortCircuitWhenPodIsDeletedDuringWaitAgainstRealInformer() {
        var pod = new PodBuilder()
            .withNewMetadata()
            .withName("pending-pod-init-deleted")
            .withNamespace("default")
            .endMetadata()
            .withNewSpec()
            .addNewInitContainer()
            .withName("init-files")
            .endInitContainer()
            .addNewContainer()
            .withName("main")
            .endContainer()
            .endSpec()
            .withNewStatus()
            .withPhase("Pending")
            .endStatus()
            .build();

        client.pods().inNamespace("default").resource(pod).create();

        var executor = Executors.newSingleThreadScheduledExecutor();
        try {
            executor.schedule(() -> {
                client.pods().inNamespace("default").withName("pending-pod-init-deleted").withGracePeriod(0).delete();
            }, 1, TimeUnit.SECONDS);

            long start = System.currentTimeMillis();
            var exception = assertThrows(
                KubernetesClientException.class,
                () -> PodService.waitForInitContainerRunning(client, pod, "init-files", Duration.ofSeconds(60))
            );
            long elapsed = System.currentTimeMillis() - start;

            assertThat(exception.getMessage(), is("Pod 'pending-pod-init-deleted' no longer exists while waiting for init container to start; it was deleted or evicted outside this task, check the namespace events"));
            assertTrue(elapsed < 10_000, "Expected wait to short-circuit in under 10s on deletion, but took " + elapsed + "ms");
        } finally {
            executor.shutdownNow();
        }
    }
}

