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
import org.slf4j.Logger;

public class PodLogService implements AutoCloseable {
    /**
     * Budget for the scheduled tick to unwind before the caller is allowed to close the
     * KubernetesClient. A tick can be blocked inside a fabric8 log read that ignores interrupts,
     * so this bounds how long {@link #close()} waits rather than waiting forever.
     */
    private static final long TERMINATION_TIMEOUT_SECONDS = 10;

    // Guarded by podLogsLock: the scheduled tick adds watches while close() drains them.
    private final List<LogWatch> podLogs = new ArrayList<>();
    private final Object podLogsLock = new Object();
    private ScheduledExecutorService scheduledExecutor;
    @Getter
    private LoggingOutputStream outputStream;
    private Thread thread;
    private ScheduledFuture<?> scheduledFuture;
    private Clock clock = Clock.systemUTC();
    private long refreshInterval = 30;
    private AbstractLogConsumer logConsumer;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private Logger logger;

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
        this.logger = logger;

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

                    if (closing.get()) {
                        // close() already drained the watches: stop here instead of opening a
                        // stream nobody will ever close (it would keep holding the client).
                        return;
                    }
                    closePodLogs();

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
                            LogWatch watch = podResource
                                    .inContainer(container.getName())
                                    // The runner already waits for the pod to become Ready with its full
                                    // waitUntilRunning budget (PodService#waitForPodReady /
                                    // waitForContainersStartedOrCompleted) before log streaming starts, so
                                    // fabric8's internal pod-Ready re-wait here is redundant dead-time. Left
                                    // enabled, it defaults to the global 30s request timeout set in
                                    // PodService#client() and silently truncates that budget. This path already
                                    // tolerates transient stream failures via the scheduled re-poll / 404
                                    // handling above, so attaching without the redundant ready-wait is safe.
                                    .withReadyWaitTimeout(0)
                                    .usingTimestamps()
                                    .sinceTime(
                                        lastTimestamp != null ? lastTimestamp.plusNanos(1).toString() : null
                                    )
                                    .watchLog(outputStream);
                            registerPodLog(watch);
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
                // Loud on purpose: a silent return here previously cost two escalations to diagnose.
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
                    // See withReadyWaitTimeout(0) comment on watchLog() above: the pod is already
                    // known Ready by the time we fetch final logs, so skip fabric8's redundant
                    // internal pod-Ready wait to avoid inheriting the 30s global request timeout.
                    .withReadyWaitTimeout(0)
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

        // Closing the watches unblocks a tick that is stuck inside a log read: an interrupt alone
        // does not wake fabric8's blocking stream.
        closePodLogs();

        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
            // A tick may still be inside a client call. Returning while it runs lets the caller
            // close the KubernetesClient underneath an open stream, which leaks the request
            // (and its threads) past the end of the task.
            awaitTermination();
        }

        // Drain anything a tick registered after the first pass: registerPodLog() closes late
        // watches itself, so this is a belt-and-braces sweep for the window in between.
        closePodLogs();

        if (thread != null) {
            thread.interrupt();
            thread = null;
        }
    }

    private void registerPodLog(LogWatch watch) {
        synchronized (podLogsLock) {
            if (!closing.get()) {
                podLogs.add(watch);
                return;
            }
        }
        // The service is closing: nobody will ever drain this watch, so close it right away.
        closeQuietly(watch);
    }

    private void closePodLogs() {
        List<LogWatch> toClose;
        synchronized (podLogsLock) {
            if (podLogs.isEmpty()) {
                return;
            }
            toClose = new ArrayList<>(podLogs);
            podLogs.clear();
        }
        toClose.forEach(PodLogService::closeQuietly);
    }

    private static void closeQuietly(LogWatch watch) {
        try {
            watch.close();
        } catch (Exception ignored) {
            // best-effort: a watch on an already-closed client throws
        }
    }

    private void awaitTermination() {
        if (scheduledExecutor == null) {
            return;
        }
        try {
            if (!scheduledExecutor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                && logger != null) {
                logger.warn(
                    "Log collection executor did not terminate within {}s; the Kubernetes client may be " +
                        "closed while a log stream is still open",
                    TERMINATION_TIMEOUT_SECONDS
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
