package io.kestra.plugin.kubernetes.shared.models;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.flows.State;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
@Isolated // testInheritClusterConfigSeedsUnsetFieldsFromAutoConfig mutates process-global kubernetes.* system properties
class ConnectionTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testConcurrencyPropertiesApplied() throws Exception {
        var connection = Connection.builder()
            .maxConcurrentRequests(Property.ofValue(16))
            .maxConcurrentRequestsPerHost(Property.ofValue(2))
            .watchReconnectInterval(Property.ofValue(Duration.ofSeconds(5)))
            .build();

        var config = connection.toConfig(runContext());

        assertThat(config.getMaxConcurrentRequests()).isEqualTo(16);
        assertThat(config.getMaxConcurrentRequestsPerHost()).isEqualTo(2);
        assertThat(config.getWatchReconnectInterval()).isEqualTo(5000);
    }

    @Test
    void testHttp2OptInAndKeepalive() throws Exception {
        var connection = Connection.builder()
            .enableHttp2(Property.ofValue(true))
            .websocketPingInterval(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        var config = connection.toConfig(runContext());

        assertThat(config.isHttp2Disable()).isFalse();
        assertThat(config.getWebsocketPingInterval()).isEqualTo(30000L);
        assertThat(connection.useOkHttpBackend(runContext())).isTrue();
    }

    @Test
    void testHttp11AndVertxBackendByDefault() throws Exception {
        var connection = Connection.builder().build();

        assertThat(connection.toConfig(runContext()).isHttp2Disable()).isTrue();
        assertThat(connection.useOkHttpBackend(runContext())).isFalse();
    }

    @Test
    void testInheritClusterConfigSeedsUnsetFieldsFromAutoConfig() throws Exception {
        // inheritClusterConfig=true seeds unset fields from ambient auto-config; false (default) starts
        // blank, the prior behavior (non-breaking). kube config + service account loading are disabled
        // so only the sys-prop master URL feeds auto-config (deterministic on any machine); masterUrl
        // is nulled so the seeded value shows through.
        System.setProperty("kubernetes.auth.tryKubeConfig", "false");
        System.setProperty("kubernetes.auth.tryServiceAccount", "false");
        System.setProperty("kubernetes.master", "https://seed-from-autoconfig:6443");
        try {
            var connection = Connection.builder()
                .masterUrl(null)
                .enableHttp2(Property.ofValue(true))
                .build();

            assertThat(connection.toConfig(runContext(), true).getMasterUrl()).contains("seed-from-autoconfig:6443");
            assertThat(connection.toConfig(runContext(), false).getMasterUrl()).doesNotContain("seed-from-autoconfig");
        } finally {
            System.clearProperty("kubernetes.master");
            System.clearProperty("kubernetes.auth.tryKubeConfig");
            System.clearProperty("kubernetes.auth.tryServiceAccount");
        }
    }

    @Test
    void testConcurrencyDefaultsPreservedWhenUnset() throws Exception {
        var connection = Connection.builder().build();

        var config = connection.toConfig(runContext());

        assertThat(config.getMaxConcurrentRequests()).isEqualTo(64);
        assertThat(config.getMaxConcurrentRequestsPerHost()).isEqualTo(5);
        assertThat(config.getWatchReconnectInterval()).isEqualTo(1000);
    }

    private RunContext runContext() {
        var task = new Task() {
            @Override
            public String getId() {
                return "test-task";
            }

            @Override
            public String getType() {
                return "Task";
            }
        };
        var taskRunId = IdUtils.create();
        var taskRun = TaskRun.builder()
            .id(taskRunId)
            .taskId("test-task")
            .flowId("test-flow")
            .namespace("test-ns")
            .executionId("test-execution")
            .state(new State().withState(State.Type.RUNNING))
            .build();
        var flow = Flow.builder()
            .id("test-flow")
            .namespace("test-ns")
            .revision(1)
            .tasks(List.of(task))
            .build();
        var execution = Execution.builder()
            .flowId("test-flow")
            .namespace("test-ns")
            .id("test-execution")
            .taskRunList(List.of(taskRun))
            .state(new State().withState(State.Type.RUNNING))
            .build();
        return runContextFactory.of(flow, task, execution, taskRun);
    }
}
