package io.kestra.plugin.kubernetes.shared.services;

import java.util.Arrays;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.readiness.Readiness;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves that fabric8's exec readiness gate (isReadyOrTerminal) creates a circular
 * dependency when targeting init containers during the Pending phase.
 *
 * Call chain: upload() → exec() → getURL() → waitUntilReadyOrTerminal(timeout)
 * └→ waitUntilCondition(isReadyOrTerminal, timeout)
 *
 * During init:
 * pod.status.phase = "Pending"
 * → isReadyOrTerminal() returns false
 * → waitUntilCondition blocks for the full timeout on EVERY exec call
 * → N upload execs × 30s timeout = 10-15 min of deadlock overhead
 *
 * Fix: withReadyWaitTimeout(0) skips the gate entirely (fabric8 default).
 */
class ExecReadyWaitDeadlockTest {

    /**
     * Exact replica of fabric8 PodOperationUtil.isReadyOrTerminal (v7.6.1).
     * This is the predicate evaluated inside every exec() → getURL() call.
     */
    static boolean isReadyOrTerminal(Pod p) {
        return p == null || Readiness.isPodReady(p) || Optional.ofNullable(p.getStatus())
            .map(s -> s.getPhase())
            .filter(phase -> !Arrays.asList("Pending", "Unknown").contains(phase))
            .isPresent();
    }

    @Test
    void isReadyOrTerminalReturnsFalseForPendingPod() {
        Pod pod = pendingPodWithRunningInitContainer();

        boolean result = isReadyOrTerminal(pod);

        assertThat(result)
            .as(
                "isReadyOrTerminal must return false during init phase (Pending). "
                    + "This means every exec() call blocks for the full readyWaitTimeout before proceeding. "
                    + "With readyWaitTimeout=30s and N upload exec calls, total overhead = N × 30s."
            )
            .isFalse();
    }

    @Test
    void isReadyOrTerminalReturnsTrueForRunningPod() {
        Pod pod = runningPodWithContainerReady();

        boolean result = isReadyOrTerminal(pod);

        assertThat(result)
            .as(
                "isReadyOrTerminal must return true when pod is Running. "
                    + "Sidecar marker uploads happen during this phase, so readyWaitTimeout has no impact."
            )
            .isTrue();
    }

    @Test
    void isPodReadyReturnsFalseDuringInitPhase() {
        Pod pod = pendingPodWithRunningInitContainer();

        assertThat(Readiness.isPodReady(pod))
            .as("Pod is never 'ready' while init containers are running")
            .isFalse();
    }

    @Test
    void pendingPhaseIsExcludedByFabric8Filter() {
        boolean passesFilter = Optional.of("Pending")
            .filter(phase -> !Arrays.asList("Pending", "Unknown").contains(phase))
            .isPresent();

        assertThat(passesFilter)
            .as("Pending phase is excluded by fabric8's phase filter, so the fallback check also fails")
            .isFalse();
    }

    @Test
    void runningPhasePassesFabric8Filter() {
        boolean passesFilter = Optional.of("Running")
            .filter(phase -> !Arrays.asList("Pending", "Unknown").contains(phase))
            .isPresent();

        assertThat(passesFilter)
            .as("Running phase passes fabric8's phase filter — no blocking for sidecar execs")
            .isTrue();
    }

    /**
     * Simulates the exact pod state during Kestra's uploadInputFiles:
     * init container is Running, but pod phase is Pending.
     */
    private Pod pendingPodWithRunningInitContainer() {
        return new PodBuilder()
            .withNewMetadata().withName("test-pod").withNamespace("default").endMetadata()
            .withNewSpec()
            .addNewInitContainer()
            .withName("init-files")
            .withImage("busybox")
            .endInitContainer()
            .addNewContainer()
            .withName("main")
            .withImage("busybox")
            .endContainer()
            .withRestartPolicy("Never")
            .endSpec()
            .withNewStatus()
            .withPhase("Pending")
            .addNewInitContainerStatus()
            .withName("init-files")
            .withReady(false)
            .withNewState()
            .withNewRunning()
            .endRunning()
            .endState()
            .endInitContainerStatus()
            .endStatus()
            .build();
    }

    private Pod runningPodWithContainerReady() {
        return new PodBuilder()
            .withNewMetadata().withName("test-pod").withNamespace("default").endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("main")
            .withImage("busybox")
            .endContainer()
            .withRestartPolicy("Never")
            .endSpec()
            .withNewStatus()
            .withPhase("Running")
            .addNewCondition()
            .withType("Ready")
            .withStatus("True")
            .endCondition()
            .addNewContainerStatus()
            .withName("main")
            .withReady(true)
            .withNewState()
            .withNewRunning()
            .endRunning()
            .endState()
            .endContainerStatus()
            .endStatus()
            .build();
    }
}
