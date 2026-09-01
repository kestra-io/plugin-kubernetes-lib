package io.kestra.plugin.kubernetes.shared.services;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.ThreadMainFactoryBuilder;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import lombok.Getter;

public class PodLogService implements AutoCloseable {
    private List<LogWatch> podLogs = new ArrayList<>();
    private ScheduledExecutorService scheduledExecutor;
    @Getter
    private LoggingOutputStream outputStream;
    private Thread thread;
    private ScheduledFuture<?> scheduledFuture;
    private Clock clock = Clock.systemUTC();
    private long refreshInterval = 30;
    private AbstractLogConsumer logConsumer;
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public void setLogConsumer(AbstractLogConsumer logConsumer) {
        this.logConsumer = logConsumer;
        if (outputStream == null) {
            outputStream = new LoggingOutputStream(logConsumer);
        }
    }

    // Visible for testing
    void setClock(Clock clock) {
        this.clock = clock;
    }

    // Visible for testing
    void setRefreshInterval(long refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    /**
     * Watches logs from every container of the pod.
     */
    public final void watch(KubernetesClient client, Pod pod, AbstractLogConsumer logConsumer, RunContext runContext) {
        watch(client, pod, logConsumer, runContext, null);
    }

    /**
     * Watches pod logs, restricted to {@code containerName} when non-null, all containers otherwise.
     */
    public final void watch(KubernetesClient client, Pod pod, AbstractLogConsumer logConsumer, RunContext runContext, String containerName) {

        var logger = runContext.logger();

        closing.set(false);
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor(ThreadMainFactoryBuilder.build("k8s-log"));
        setLogConsumer(logConsumer);
        AtomicBoolean started = new AtomicBoolean(false);
        AtomicReference<Instant> lastReconnection = new AtomicReference<>(Instant.now(clock));
        AtomicReference<String> lastReconnectLogKey = new AtomicReference<>();
        // The task runs with initial delay 0, so its first tick can fire before the
        // scheduleAtFixedRate() return value is assigned. The 404 branch publishes its
        // cancel intent through stopRequested BEFORE reading futureRef, and the scheduling
        // thread publishes futureRef BEFORE reading stopRequested - whichever side loses
        // the race, at least one of them observes the other and performs the cancel.
        AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
        AtomicBoolean stopRequested = new AtomicBoolean(false);

        scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
            () ->
            {
                Instant lastTimestamp = outputStream.getLastTimestamp() == null ? null : Instant.from(outputStream.getLastTimestamp());
                boolean forceReconnect = Instant.now(clock).isAfter(lastReconnection.get().plus(Duration.ofHours(3)));
                boolean staleLogs = lastTimestamp == null || lastTimestamp.isBefore(Instant.now(clock).minus(Duration.ofMinutes(10)));

                if (!started.get() || forceReconnect || staleLogs) {
                    if (!started.get()) {
                        started.set(true);
                    } else {
                        if (forceReconnect) {
                            logger.trace("Connection is over 3 hours old, forcing reconnect to prevent kubelet disconnect.");
                        } else {
                            String reconnectLogKey = "stale:" + String.valueOf(lastTimestamp);
                            if (!Objects.equals(lastReconnectLogKey.get(), reconnectLogKey)) {
                                logger.trace("No log since '{}', reconnecting", lastTimestamp == null ? "unknown" : lastTimestamp.toString());
                                lastReconnectLogKey.set(reconnectLogKey);
                            }
                        }
                    }

                    lastReconnection.set(Instant.now(clock));

                    if (podLogs != null) {
                        podLogs.forEach(LogWatch::close);
                        podLogs = new ArrayList<>();
                    }

                    PodResource podResource = PodService.podRef(client, pod);

                    var containers = pod
                        .getSpec()
                        .getContainers()
                        .stream()
                        .filter(container -> containerName == null || containerName.equals(container.getName()))
                        .toList();

                    if (containers.isEmpty() && containerName != null) {
                        logger.warn("Unable to find container {}, no logs will be reported", containerName);
                    }

                    containers.forEach(container ->
                    {
                        try {
                            podLogs.add(
                                podResource
                                    .inContainer(container.getName())
                                    .usingTimestamps()
                                    .sinceTime(
                                        lastTimestamp != null ? lastTimestamp.plusNanos(1).toString() : null
                                    )
                                    .watchLog(outputStream)
                            );
                        } catch (KubernetesClientException e) {
                            if (e.getCode() == 404) {
                                logger.info("Pod no longer exists, stopping log collection");
                                // Publish intent first, then read the reference (see handshake comment above).
                                stopRequested.set(true);
                                ScheduledFuture<?> future = futureRef.get();
                                if (future != null) {
                                    future.cancel(false);
                                }
                            } else {
                                throw e;
                            }
                        }
                    });
                } else {
                    lastReconnectLogKey.set(null);
                }
            },
            0,
            refreshInterval,
            TimeUnit.SECONDS
        );
        futureRef.set(scheduledFuture);
        // Publish the reference first, then check the intent (see handshake comment above).
        if (stopRequested.get()) {
            scheduledFuture.cancel(false);
        }

        // look at exception on the main thread
        thread = Thread.ofVirtual().name("k8s-listener").start(
            () ->
            {
                try {
                    scheduledFuture.get();
                } catch (CancellationException e) {
                    if (!closing.get()) {
                        logger.debug("{} cancelled", this.getClass().getName(), e);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (!closing.get()) {
                        logger.debug("{} interrupted", this.getClass().getName(), e);
                    }
                } catch (ExecutionException e) {
                    logger.error("{} exception", this.getClass().getName(), e);
                }
            }
        );
    }

    public void fetchFinalLogs(KubernetesClient client, Pod pod, RunContext runContext) {
        if (outputStream == null) {
            if (logConsumer == null) {
                // Loud on purpose: a missing consumer means every container log for this pod is
                // dropped, and the only previous fingerprint was the absence of the debug line
                // below - which cost two customer escalations to diagnose.
                runContext.logger().warn(
                    "Cannot fetch final logs for pod '{}': no log consumer was set, container output for this pod will be lost. "
                    + "This indicates setLogConsumer() was not called before fetchFinalLogs().",
                    pod.getMetadata().getName()
                );
                return;
            }
            outputStream = new LoggingOutputStream(logConsumer);
        }

        Instant lastTimestamp = outputStream.getLastTimestamp();
        Instant lookbackTime = lastTimestamp != null ? lastTimestamp.minus(Duration.ofSeconds(60)) : null;

        // Hybrid approach: fetch logs since (lastTimestamp - 60s) to catch any missed by watchLog()
        // - Provides 60s safety buffer for multi-container out-of-order logs and K8s API delays
        // - Uses lastTimestamp as anchor for unbounded lookback when watchLog() failures are prolonged
        // - Hash-based deduplication efficiently handles the increased overlap
        runContext.logger().debug(
            "Fetching final logs since lookbackTime={} (lastTimestamp={} minus 60s)",
            lookbackTime,
            lastTimestamp
        );

        PodResource podResource = PodService.podRef(client, pod);

        if (pod.getSpec().getContainers().stream().noneMatch(container ->
        {
            try {
                String logs = podResource
                    .inContainer(container.getName())
                    .usingTimestamps()
                    .sinceTime(lookbackTime != null ? lookbackTime.toString() : null)
                    .getLog();

                if (logs != null && !logs.isEmpty()) {
                    outputStream.write(logs.getBytes());
                    outputStream.flush();
                    return true;
                }
                runContext.logger().debug("No logs returned for container '{}'", container.getName());
            } catch (IOException e) {
                runContext.logger().error("Failed to fetch final logs for container '{}'", container.getName(), e);
            }
            return false;
        })) {
            // if no container logs were found, the pod likely never started.
            // we fall back to Kubernetes pod events
            fetchPodEvents(client, pod, runContext);
        }
    }

    private void fetchPodEvents(KubernetesClient client, Pod pod, RunContext runContext) {
        try {
            var events = client.v1().events()
                .inNamespace(pod.getMetadata().getNamespace())
                .withField("involvedObject.name", pod.getMetadata().getName())
                .list()
                .getItems();

            if (events.isEmpty()) {
                runContext.logger().warn("No container logs and no pod events found for pod '{}'", pod.getMetadata().getName());
                return;
            }

            runContext.logger().info("No container logs available. Pod events:");
            for (var event : events) {
                outputStream.write(("[pod-event] " + event.getReason() + ": " + event.getMessage() + "\n").getBytes());
            }
            outputStream.flush();
        } catch (Exception e) {
            runContext.logger().error("Failed to fetch pod events for '{}'", pod.getMetadata().getName(), e);
        }
    }

    @Override
    public void close() throws IOException {
        closing.set(true);

        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }

        // Ensure the scheduled task reaches a terminal state to avoid blocking on future.get() in the listener
        if (scheduledFuture != null) {
            try {
                scheduledFuture.cancel(true);
            } catch (Exception ignore) {
                // best-effort cancellation
            }
        }

        if (thread != null) {
            thread.interrupt();
            thread = null;
        }

        if (podLogs != null) {
            podLogs.forEach(LogWatch::close);
        }

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
    }
}
