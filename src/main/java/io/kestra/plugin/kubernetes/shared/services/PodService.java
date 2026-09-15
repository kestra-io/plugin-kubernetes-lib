package io.kestra.plugin.kubernetes.shared.services;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.retrys.Exponential;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.TaskException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.RetryUtils;
import io.kestra.plugin.kubernetes.shared.models.Connection;
import io.kestra.plugin.kubernetes.shared.models.SideCar;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.PodResource;

public final class PodService {
    private PodService() {
    }

    private static final List<String> COMPLETED_PHASES = List.of(PodPhase.SUCCEEDED.value(), PodPhase.FAILED.value(), PodPhase.UNKNOWN.value()); // see https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase
    private static final String SIDECAR_FILES_CONTAINER_NAME = "out-files";

    // EE's marker for "upload the whole working directory" in uploadInputFiles — see its Javadoc.
    private static final Path EMPTY_RELATIVE_PATH = Path.of("");

    // Default exec WebSocket timeout in fabric8 is 10s, which is too short for pods with security contexts
    private static final int REQUEST_TIMEOUT_MS = 30_000;

    // 0 means "don't wait at all"; 30s gives the pod time to become ready before exec connections are established
    public static final int EXEC_READY_WAIT_TIMEOUT_MS = 30_000;

    private static final int UPLOAD_RETRY_MAX_ATTEMPTS = 5;

    public static final Duration DEFAULT_RETRY_MAX_DURATION = Duration.ofSeconds(60);

    private static final String INIT_FILES_CONTAINER_NAME = "init-files";

    private static final String READY_MARKER = "ready";

    private static final Duration UPLOAD_VERIFICATION_TIMEOUT = Duration.ofSeconds(10);

    public static KubernetesClient client(RunContext runContext, Connection connection) throws IllegalVariableEvaluationException {
        return client(runContext, connection, false);
    }

    public static KubernetesClient client(RunContext runContext, Connection connection, boolean inheritClusterConfig) throws IllegalVariableEvaluationException {
        if (connection == null) {
            return client((Config) null);
        }
        Config config = connection.toConfig(runContext, inheritClusterConfig);
        config.setRequestTimeout(REQUEST_TIMEOUT_MS);
        return ClientService.of(config, connection.useOkHttpBackend(runContext));
    }

    public static KubernetesClient client(Config config) {
        if (config == null) {
            config = Config.autoConfigure(null);
        }
        config.setRequestTimeout(REQUEST_TIMEOUT_MS);
        return ClientService.of(config);
    }

    public static Pod waitForInitContainerRunning(KubernetesClient client, Pod pod, String container, Duration waitUntilRunning) {
        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j != null &&
                    j.getStatus() != null &&
                    j.getStatus().getInitContainerStatuses() != null &&
                    j.getStatus()
                        .getInitContainerStatuses()
                        .stream()
                        .filter(containerStatus -> containerStatus.getName().equals(container))
                        .anyMatch(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getRunning() != null),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static Pod waitForPodReady(KubernetesClient client, Pod pod, Duration waitUntilRunning) {
        boolean hasSidecar = pod.getSpec().getContainers().stream()
            .anyMatch(c -> SIDECAR_FILES_CONTAINER_NAME.equals(c.getName()));

        return PodService.podRef(client, pod)
            .waitUntilCondition(
                j -> j != null &&
                    j.getStatus() != null && (PodPhase.FAILED.value().equals(j.getStatus().getPhase()) ||
                        (j.getStatus().getContainerStatuses() != null &&
                            j.getStatus().getContainerStatuses().stream()
                                .anyMatch(
                                    cs -> cs.getState() != null &&
                                        cs.getState().getWaiting() != null &&
                                        cs.getState().getWaiting().getReason() != null &&
                                        !TransientWaitingReason.contains(cs.getState().getWaiting().getReason())
                                ))
                        ||
                        j.getStatus()
                            .getConditions()
                            .stream()
                            .anyMatch(
                                podCondition -> ("ContainersReady".equals(podCondition.getType()) &&
                                    (hasSidecar || "True".equals(podCondition.getStatus()))) ||
                                    ("PodCompleted".equals(podCondition.getReason()))
                            )),
                waitUntilRunning.toSeconds(),
                TimeUnit.SECONDS
            );
    }

    public static boolean isCompletedPhase(String phase) {
        return phase != null && COMPLETED_PHASES.contains(phase);
    }

    private static boolean isStartedOrCompleted(Pod pod) {
        if (pod == null || pod.getStatus() == null) {
            return false;
        }
        var phase = pod.getStatus().getPhase();
        if (isCompletedPhase(phase)) {
            return true;
        }
        return PodPhase.RUNNING.value().equals(phase) &&
            pod.getStatus().getContainerStatuses() != null &&
            pod.getStatus().getContainerStatuses().stream()
                .anyMatch(c -> c.getState() != null && c.getState().getRunning() != null);
    }

    public static Pod waitForContainersStartedOrCompleted(KubernetesClient client, Logger logger, Pod pod, Duration waitUntilRunning) {
        var podResource = PodService.podRef(client, pod);

        // Fast path: pod already satisfies the condition (e.g. on resume with a Running/Succeeded pod)
        var current = podResource.get();
        if (current == null) {
            throw new KubernetesClientException("Pod was deleted while waiting for containers to start: " + pod.getMetadata().getName());
        }
        if (isStartedOrCompleted(current)) {
            return current;
        }

        var remaining = waitUntilRunning;
        var chunk = Duration.ofMinutes(5);

        while (remaining.toSeconds() > 0) {
            var waitTime = remaining.compareTo(chunk) < 0 ? remaining : chunk;
            remaining = remaining.minus(waitTime);

            try {
                var result = podResource.waitUntilCondition(
                    PodService::isStartedOrCompleted,
                    waitTime.toSeconds(),
                    TimeUnit.SECONDS
                );
                if (result != null) {
                    return result;
                }
            } catch (KubernetesClientTimeoutException e) {
                // chunk expired — fall through to GET check
            } catch (KubernetesClientException e) {
                // watch error — fall through to GET check
                logger.debug("Watch error while waiting for containers to start after {}s, rechecking pod state", waitTime.toSeconds(), e);
            }

            podResource = podRef(client, pod);
            var polled = podResource.get();
            if (polled == null) {
                throw new KubernetesClientException("Pod was deleted while waiting for containers to start: " + pod.getMetadata().getName());
            }
            if (isStartedOrCompleted(polled)) {
                return polled;
            }
        }

        throw new KubernetesClientTimeoutException(pod, waitUntilRunning.toSeconds(), TimeUnit.SECONDS);
    }

    public static Pod waitForCompletionExcept(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning, String except) {
        return waitForCompletion(
            client,
            logger,
            pod,
            waitRunning,
            j -> j != null &&
                j.getStatus() != null &&
                j.getStatus().getContainerStatuses() != null &&
                j.getStatus()
                    .getContainerStatuses()
                    .stream()
                    .anyMatch(containerStatus -> !containerStatus.getName().equals(except))
                &&
                j.getStatus()
                    .getContainerStatuses()
                    .stream()
                    .filter(containerStatus -> !containerStatus.getName().equals(except))
                    .allMatch(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
        );
    }

    public static Pod waitForCompletion(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning) {
        return waitForCompletion(
            client,
            logger,
            pod,
            waitRunning,
            j -> j != null &&
                j.getStatus() != null &&
                COMPLETED_PHASES.contains(j.getStatus().getPhase())
        );
    }

    public static Pod waitForCompletion(KubernetesClient client, Logger logger, Pod pod, Duration waitRunning, Predicate<Pod> condition) {
        var podResource = podRef(client, pod);
        var remaining = waitRunning;
        var chunk = Duration.ofMinutes(5);

        while (remaining.toSeconds() > 0) {
            var waitTime = remaining.compareTo(chunk) < 0 ? remaining : chunk;
            remaining = remaining.minus(waitTime);

            try {
                var ended = podResource.waitUntilCondition(condition, waitTime.toSeconds(), TimeUnit.SECONDS);
                if (ended != null) {
                    return ended;
                }
            } catch (KubernetesClientTimeoutException e) {
                // chunk expired — fall through to GET check below
            } catch (KubernetesClientException e) {
                // watch error — fall through to GET check below
                logger.debug("Watch error while waiting for pod completion, checking pod state", e);
            }

            podResource = podRef(client, pod);
            var current = podResource.get();
            if (current == null) {
                var podName = pod.getMetadata().getName();
                logger.warn("Pod '{}' was deleted before reaching a terminal phase", podName);
                throw new KubernetesClientException("Pod was deleted before reaching a terminal phase: " + podName);
            }
            if (condition.test(current)) {
                return current;
            }
        }

        throw new KubernetesClientTimeoutException(pod, waitRunning.toSeconds(), TimeUnit.SECONDS);
    }

    public static IllegalStateException failedMessage(Pod pod) throws IllegalStateException {
        if (pod.getStatus() == null) {
            return new IllegalStateException("Pods terminated without any status !");
        }

        return (pod.getStatus().getContainerStatuses() == null ? new ArrayList<ContainerStatus>() : pod.getStatus().getContainerStatuses())
            .stream()
            .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
            .map(containerStatus -> containerStatus.getState().getTerminated())
            .findFirst()
            .map(
                containerStateTerminated -> new IllegalStateException(
                    "Pods terminated with status '" + pod.getStatus().getPhase() + "', " +
                        "exitcode '" + containerStateTerminated.getExitCode() + "' & " +
                        "message '" + containerStateTerminated.getMessage() + "'"
                )
            )
            .orElseGet(() ->
            {
                if (pod.getStatus().getContainerStatuses() != null) {
                    Optional<String> waitingReason = pod.getStatus().getContainerStatuses().stream()
                        .filter(cs -> cs.getState() != null && cs.getState().getWaiting() != null)
                        .map(cs -> cs.getState().getWaiting().getReason())
                        .filter(reason -> reason != null && !TransientWaitingReason.contains(reason))
                        .findFirst();
                    if (waitingReason.isPresent()) {
                        return new IllegalStateException("Pod failed before container start: " + waitingReason.get());
                    }
                }
                return new IllegalStateException("Pod failed with phase '" + pod.getStatus().getPhase() + "'");
            });
    }

    /**
     * Checks terminated containers for non-zero exit codes and throws an {@link IllegalStateException}
     * describing the first failure found.
     */
    public static void checkContainerFailures(Pod pod, String exceptContainer, Logger logger) throws IllegalStateException {
        Optional<ContainerStatus> failed = findFailedContainer(pod, exceptContainer);
        if (failed.isEmpty()) {
            return;
        }

        String errorMsg = containerFailureMessage(failed.get());
        logger.error(errorMsg);
        throw new IllegalStateException(errorMsg);
    }

    /**
     * Checks terminated containers for non-zero exit codes and throws a {@link TaskException}
     * carrying the log consumer, for task-runner callers.
     */
    public static void checkContainerFailures(Pod pod, String exceptContainer, Logger logger, AbstractLogConsumer defaultLogConsumer) throws TaskException {
        Optional<ContainerStatus> failed = findFailedContainer(pod, exceptContainer);
        if (failed.isEmpty()) {
            return;
        }

        logger.error(containerFailureMessage(failed.get()));
        throw new TaskException(-1, defaultLogConsumer);
    }

    private static Optional<ContainerStatus> findFailedContainer(Pod pod, String exceptContainer) {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return Optional.empty();
        }

        return pod.getStatus().getContainerStatuses().stream()
            .filter(containerStatus -> !containerStatus.getName().equals(exceptContainer))
            .filter(containerStatus -> containerStatus.getState() != null && containerStatus.getState().getTerminated() != null)
            .filter(containerStatus -> containerStatus.getState().getTerminated().getExitCode() != 0)
            .findFirst();
    }

    private static String containerFailureMessage(ContainerStatus containerStatus) {
        ContainerStateTerminated terminated = containerStatus.getState().getTerminated();
        return "Container '" + containerStatus.getName() + "' failed with exit code " +
            terminated.getExitCode() +
            (terminated.getReason() != null ? ", reason: " + terminated.getReason() : "") +
            (terminated.getMessage() != null ? ", message: " + terminated.getMessage() : "");
    }

    public static PodResource podRef(KubernetesClient client, Pod pod) {
        return client.pods()
            .inNamespace(pod.getMetadata().getNamespace())
            .withName(pod.getMetadata().getName());
    }

    /**
     * Retry file operations with exponential backoff optimized for freshly provisioned nodes,
     * capped at a 60s budget.
     */
    public static Boolean withRetries(Logger logger, String where, RetryUtils.CheckedSupplier<Boolean> call) throws IOException {
        return withRetries(logger, where, call, DEFAULT_RETRY_MAX_DURATION);
    }

    /**
     * Retry file operations with exponential backoff, capped at {@code maxDuration}. Use a shorter
     * budget on best-effort paths (e.g. output-file download from an already-failed pod) so a call
     * that cannot succeed does not burn the full default window.
     */
    public static Boolean withRetries(Logger logger, String where, RetryUtils.CheckedSupplier<Boolean> call, Duration maxDuration) throws IOException {
        var attempt = new AtomicInteger(0);
        try {
            return RetryUtils.Instance.<Boolean, IOException> builder()
                .policy(
                    Exponential.builder()
                        .delayFactor(2.0)
                        .interval(Duration.ofSeconds(1))
                        .maxInterval(Duration.ofSeconds(10))
                        .maxDuration(maxDuration)
                        .maxAttempts(UPLOAD_RETRY_MAX_ATTEMPTS)
                        .build()
                )
                .logger(logger)
                // On exhaustion, Failsafe's Fallback throws RetryUtils.RetryFailed rather than an IOException,
                // which would otherwise discard the detailed hint message thrown below. Unwrap it back to the
                // last real cause so that detail survives all the way to the caller instead of being replaced
                // by a bare "Failed to call" message.
                .failureFunction(retryFailed -> retryFailed.getCause() instanceof IOException ioException
                    ? ioException
                    : new IOException("Failed to call '" + where + "'", retryFailed.getCause()))
                .build()
                // Stay on the result-predicate overload (rather than run(IOException.class, ...), which would
                // narrow Failsafe's default "retry on any exception" to IOException only): fabric8 calls can
                // throw KubernetesClientException and other non-IOException runtime errors that must still be
                // retried the same way a bare `false` result is.
                .run(
                    object -> !object,
                    () ->
                    {
                        var currentAttempt = attempt.incrementAndGet();

                        if (!call.get()) {
                            // fabric8's exec/copy calls return a bare boolean and swallow the underlying exec/tar
                            // error, so there is no exception or stderr to surface here. Throwing here — instead
                            // of just returning false, as before — makes this failure the retry policy's tracked
                            // "last exception", so the detailed hint below is what the failureFunction above
                            // unwraps on exhaustion, rather than a bare RetryFailed with no cause to unwrap.
                            throw new IOException(
                                "Failed to call '" + where + "' after " + currentAttempt + "/" + UPLOAD_RETRY_MAX_ATTEMPTS +
                                    " attempts — the Kubernetes copy/exec call kept reporting failure without further detail; " +
                                    "verify connectivity to the pod and that the file-sidecar image provides a working 'sh' and 'tar'"
                            );
                        }

                        return true;
                    }
                );
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException("Failed to call '" + where + "'", e);
        }
    }

    /**
     * Runs a command inside a container and returns its stdout, but only if it exits with code 0.
     * Used to cross-check fabric8 copy/upload results, which can report success even when the
     * underlying tar transfer was truncated.
     * <p>
     * On timeout, only the client-side {@code ExecWatch} is closed (via try-with-resources); fabric8's exec
     * API has no way to signal the remote process, so it may keep running inside the (short-lived,
     * verification-only) sidecar container until that container is torn down with the rest of the pod.
     * <p>
     * Every fabric8 {@code exec} call re-waits for the whole pod to report Ready (up to
     * {@link #EXEC_READY_WAIT_TIMEOUT_MS}) before opening the exec connection — see
     * {@code PodOperationsImpl#getURL}. That wait is structurally doomed to run to its full timeout here:
     * the pod can't be Ready while the init/sidecar file-transfer containers are still blocked on marker
     * files, which is exactly when upload verification runs. Since this call always follows an already-successful
     * exec on the very same container (the upload itself), the container is proven reachable, so skip that
     * redundant wait entirely instead of paying it again on every verification call.
     */
    public static Optional<String> execOutput(ContainerResource container, Logger logger, Duration timeout, String... command) {
        var output = new ByteArrayOutputStream();
        try (var watch = container.withReadyWaitTimeout(0).writingOutput(output).exec(command)) {
            var exitCode = watch.exitCode().get(timeout.toSeconds(), TimeUnit.SECONDS);
            if (exitCode == null || exitCode != 0) {
                return Optional.empty();
            }
            return Optional.of(output.toString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            // Restore the flag so the caller can observe the cancellation, and fail loudly instead of
            // returning Optional.empty(): that's the same shape as "sidecar lacks find/wc" below, which
            // verifyUpload treats as a skipped-not-failed check — silently letting a cancelled task's
            // verification loop carry on to the next file group instead of aborting.
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new IOException(
                "Verification command '" + String.join(" ", command) + "' was interrupted", e
            ));
        } catch (TimeoutException e) {
            // Best-effort: a timeout here is otherwise indistinguishable from "verification passed", so
            // log it explicitly rather than silently skipping the check like the generic catch below.
            logger.info(
                "Verification command '{}' timed out after {}s in the file-sidecar container, skipping upload verification",
                String.join(" ", command), timeout.toSeconds()
            );
            return Optional.empty();
        } catch (Exception e) {
            logger.debug("Verification command '{}' could not be executed in the file-sidecar container", String.join(" ", command), e);
            return Optional.empty();
        }
    }

    /**
     * Retries a fallible verification check (e.g. a per-file upload size/count comparison), applying the
     * same retry treatment given to the raw upload/exec calls above so a single transient exec failure
     * during verification doesn't turn an otherwise-good upload into a hard task failure.
     */
    public static void withVerificationRetries(Logger logger, String where, CheckedRunnable check) throws IOException {
        try {
            RetryUtils.Instance.<Void, IOException> builder()
                .policy(
                    Exponential.builder()
                        .delayFactor(2.0)
                        .interval(Duration.ofSeconds(1))
                        .maxInterval(Duration.ofSeconds(10))
                        .maxDuration(Duration.ofSeconds(60))
                        .maxAttempts(UPLOAD_RETRY_MAX_ATTEMPTS)
                        .build()
                )
                .logger(logger)
                // On exhaustion, Failsafe's Fallback throws RetryUtils.RetryFailed rather than the checked
                // exception the policy handles, which would otherwise discard the actionable message built by
                // verifyUpload (e.g. "expected N file(s) but found M"). Unwrap it back to the last real cause
                // so that detail survives all the way to the caller instead of being replaced by a bare
                // "Failed to call" message.
                .failureFunction(retryFailed -> retryFailed.getCause() instanceof IOException ioException
                    ? ioException
                    : new IOException("Failed to call '" + where + "'", retryFailed.getCause()))
                .build()
                .run(
                    IOException.class,
                    () ->
                    {
                        check.run();
                        return null;
                    }
                );
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException("Failed to call '" + where + "'", e);
        }
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws IOException;
    }

    /**
     * Uploads a task's input files into the init-files container so they are available in the main
     * container's working directory before it starts.
     * <p>
     * An empty-path entry in {@code relativePaths} is EE's marker for "upload the whole working
     * directory in a single tar transfer" (see {@link #EMPTY_RELATIVE_PATH}); when present, that
     * whole-directory transfer is tried FIRST, before anything else. The remaining entries — grouped
     * by their top-level path segment, one group per sibling — are used ONLY as the fallback if that
     * single transfer fails verification: each sibling group is then re-uploaded on its own, while the
     * empty-path group itself is skipped (re-attempting it would just repeat the failed transfer). If
     * the empty-path group is the ONLY group present, {@code relativePaths} was just the whole-directory
     * marker with no sibling top-level groups to fall back to — NOT necessarily a directory that is
     * empty on disk. In fact this is the DEFAULT shape for an EE run with no inputFiles, no
     * namespaceFiles, and the deprecated outputDirectory property left at its default (false):
     * {@code TaskCommands.relativeWorkingDirectoryFilesPaths(true)} always creates {@code
     * CommandsWrapper.getOutputDirectory()} lazily, then filters that directory back out of the walk,
     * so {@code [""]}-alone is what a working directory with only that filtered-out entry produces.
     * Verification still passes trivially in that shape, since both the local and pod-side counts only
     * count non-directory entries. Whatever the actual on-disk shape, a failed whole-directory transfer
     * with no sibling group to fall back to is surfaced as an {@link IOException} instead of silently
     * skipping the (only) fallback candidate and reporting success — trading away main's two full retry
     * rounds for one: main's fallback loop had no {@code continue} for the empty-path group, so it
     * re-uploaded and re-verified the same empty path before throwing the same truncated IOException;
     * this version throws directly on the first failure instead of repeating that doomed retry round.
     * OSS callers never send the empty-path marker, so for them every entry is a normal top-level
     * group from the start.
     * <p>
     * Whenever a top-level group is a local directory, it is uploaded in a single tar transfer rather
     * than one file at a time — exec/copy round-trips to the pod are the dominant cost for large
     * directories (e.g. a Python virtualenv or node_modules), and tar preserves symlinks as their own
     * entry instead of dereferencing them. fabric8's tar-based directory upload can report success even
     * when the transfer was silently truncated on a slow or contended cluster, so every bulk upload
     * (the whole-directory one and each top-level group's) is cross-checked against the pod-side file
     * count ({@link #verifyDirectoryUpload}) and falls back to a slower, individually verified per-file
     * (or per-directory) upload when the counts disagree.
     * <p>
     * Transport differs by shape: a standalone regular file streams via a single cat-style {@code
     * upload(InputStream)} exec, while a local directory (whether the whole-directory bulk upload, a
     * top-level group, or a directory encountered in the per-file fallback loop) goes through a single
     * tar {@code upload(Path)} exec.
     * <p>
     * Each entry in {@code relativePaths} must stay within {@code localBaseDir}: an absolute entry, or
     * one that escapes the base directory after normalization, is rejected up-front with an {@link
     * IllegalArgumentException}. This containment check is purely lexical — {@link Path#normalize()}
     * does not touch the filesystem — so a symlink that lives under {@code localBaseDir} but points
     * outside it is not detected here and resolves through at open time, exactly as in the two consumer
     * copies this consolidates.
     *
     * @param runContext the run context, used to resolve the local marker file location for {@link #uploadMarker}
     * @param podResource the pod to upload into
     * @param logger the logger to report upload/verification progress and fallbacks to
     * @param localBaseDir the local directory that {@code relativePaths} are resolved against
     * @param containerWorkingDir the working directory inside the init-files container that
     *                             {@code relativePaths} are uploaded under
     * @param relativePaths the input files/directories to upload, relative to {@code localBaseDir}
     */
    public static void uploadInputFiles(
        RunContext runContext,
        PodResource podResource,
        Logger logger,
        Path localBaseDir,
        String containerWorkingDir,
        List<Path> relativePaths
    ) throws IOException {
        var normalizedBaseDir = localBaseDir.normalize();
        // Normalize each relative path ONCE and use the normalized form for every downstream step —
        // the containment guard, the grouping key, the local resolve, and the container path — so the
        // guard can never accept a path the upload then interprets differently. Without this, an entry
        // like './x' passes the guard (it resolves to 'x') but its raw top segment '.' makes the group
        // resolve to localBaseDir itself, tarring the ENTIRE base directory instead of the one file.
        // A bare '.' and the empty path both normalize to the empty path; this is DELIBERATELY kept,
        // not rejected: EE walks its working directory into this list and always includes an empty
        // entry that means "upload the whole working directory AND NOTHING ELSE". The other entries are
        // retained purely as the fallback set if that single whole-directory transfer fails verification
        // (see the grouping below) — without this, every top-level entry was re-sent individually on top
        // of the whole-directory transfer, doubling the bytes uploaded on every EE run.
        // Because '.' normalizes to the same empty path, a bare '.' entry takes this exact same
        // whole-working-directory path and the same grouped.size() == 1 throw below as an explicit empty
        // entry would. Neither current caller actually produces a bare '.': OSS fails earlier, in
        // PluginUtilsService.createInputFiles, on any path outside the working directory before this
        // method ever sees it, and EE only ever emits already-relativized paths. This is documented here
        // purely as an equivalence, not a bug.
        var normalizedRelatives = new ArrayList<Path>(relativePaths.size());
        for (Path relative : relativePaths) {
            if (relative.isAbsolute()) {
                throw new IllegalArgumentException("Input file path '" + relative + "' must be relative to the local base directory, but is absolute");
            }
            var resolved = normalizedBaseDir.resolve(relative).normalize();
            if (!resolved.startsWith(normalizedBaseDir)) {
                throw new IllegalArgumentException("Input file path '" + relative + "' escapes the local base directory '" + localBaseDir + "'");
            }
            // Derived from the already-validated 'resolved', not from re-normalizing 'relative' in
            // isolation: an anchor-free normalize() can leave a '..' that the anchored resolve() above
            // absorbed (e.g. 'b/../../<baseDirName>/legit.txt' resolves back in bounds but normalizes in
            // isolation to '../<baseDirName>/legit.txt'), which would make the guard validate one path
            // while grouping/uploading a different one that escapes localBaseDir.
            normalizedRelatives.add(normalizedBaseDir.relativize(resolved));
        }

        var grouped = normalizedRelatives.stream()
            .collect(Collectors.groupingBy(p -> p.getNameCount() > 0 ? p.getName(0) : p));

        // Every fabric8 exec/upload call re-waits for the whole pod to report Ready before opening the
        // connection — see PodOperationsImpl#getURL. That wait is structurally doomed to run to its full
        // timeout here: the pod can't be Ready while init-files is itself still blocked waiting for the
        // ready marker file, which is exactly the upload this call is about to perform. The caller already
        // proved this container is Running via PodService.waitForInitContainerRunning() right before this
        // call, so skip the redundant, unsatisfiable pod-Ready wait entirely instead of paying it on every
        // retry attempt.
        //
        // Do NOT restore a positive timeout here. This value has already round-tripped once: it was set to
        // 30s (bf45e73) to stop intermittent "exec endpoint not initialized yet" failures seen even while
        // the pod was Running. That concern is real, but a pod-Ready wait cannot address it at THIS call
        // site — the condition it waits on is unsatisfiable until after this very upload, so any positive
        // value is dead time that expires and proceeds anyway, never protection. Transient exec-endpoint
        // failures are covered instead by PodService.withRetries (5 attempts, 1s→10s backoff, 60s budget),
        // which only became effective once each attempt stopped burning the full timeout first.
        var container = podResource
            .inContainer(INIT_FILES_CONTAINER_NAME)
            .withReadyWaitTimeout(0);

        // The empty path is EE's "upload the whole working directory" marker (see the comment above
        // normalizedRelatives). When present, try it as a single bulk transfer instead of grouping and
        // uploading every top-level entry individually on top of it — otherwise every byte in the working
        // directory is sent twice. Only fall back to the per-top-level-group loop below if that single
        // transfer fails its verification.
        // containerWorkingDir is passed VERBATIM here, not through containerPath(containerWorkingDir,
        // EMPTY_RELATIVE_PATH) — iterating an empty Path yields one empty segment, so that helper would
        // append a trailing '/'. This is deliberate, not an oversight: a base path with no trailing slash
        // is equivalent for both 'tar -C' and 'find' (the two shell tools this value ever reaches), and
        // it matches what EE's own copy of this transfer passes.
        var wholeDirectoryUpload = grouped.containsKey(EMPTY_RELATIVE_PATH)
            ? tryBulkUploadDirectory(container, logger, normalizedBaseDir, containerWorkingDir, "the whole working directory")
            : null;

        if (wholeDirectoryUpload == null || !wholeDirectoryUpload.success()) {
            // No sibling top-level group exists to fall back to — the empty-path group is the ONLY
            // group, meaning relativePaths was just the whole-directory marker for a directory with no
            // top-level contents. Skipping it below (as its real siblings' fallback does) and proceeding
            // to uploadMarker would silently report success for a transfer that actually failed, so
            // surface the failure instead. This can only happen when wholeDirectoryUpload is non-null
            // (i.e. the marker was present and its bulk attempt actually ran and failed).
            if (wholeDirectoryUpload != null && grouped.size() == 1) {
                throw wholeDirectoryUpload.failure();
            }

            for (var entry : grouped.entrySet()) {
                // The empty-path group's own (and only) value is the empty path itself, resolving to
                // localBaseDir — re-uploading it here would just repeat the whole-directory transfer that
                // already failed above, not recover from it. Its other, real top-level siblings are the
                // actual fallback set.
                if (entry.getKey().equals(EMPTY_RELATIVE_PATH)) {
                    continue;
                }
                uploadGroup(container, normalizedBaseDir, containerWorkingDir, entry.getKey(), entry.getValue(), logger);
            }
        }

        try {
            uploadMarker(runContext, podResource, logger, READY_MARKER, INIT_FILES_CONTAINER_NAME);
        } catch (IOException e) {
            // The init container exits only when it finds /kestra/ready, so exit code 0 means
            // the marker arrived even if the exec WebSocket closed before fabric8 got a clean result.
            Pod current;
            try {
                current = podResource.get();
            } catch (RuntimeException lookupError) {
                // Status lookup failed too — surface the original upload error, not this one.
                e.addSuppressed(lookupError);
                throw e;
            }
            boolean initContainerSucceeded = current != null &&
                current.getStatus() != null &&
                current.getStatus().getInitContainerStatuses() != null &&
                current.getStatus().getInitContainerStatuses().stream()
                    .filter(cs -> INIT_FILES_CONTAINER_NAME.equals(cs.getName()))
                    .anyMatch(
                        cs -> cs.getState() != null &&
                            cs.getState().getTerminated() != null &&
                            Integer.valueOf(0).equals(cs.getState().getTerminated().getExitCode())
                    );
            if (initContainerSucceeded) {
                logger.debug("uploadMarker exec failed but init container exited with code 0, marker was received");
            } else {
                throw e;
            }
        }
    }

    /**
     * Outcome of a single {@link #tryBulkUploadDirectory} attempt: on failure, carries the IOException
     * that caused it, so a caller with no sibling fallback data of its own (the empty-path short-circuit
     * in {@link #uploadInputFiles}) can surface it instead of silently reporting success. Callers that
     * DO have a fallback (each per-top-level group in {@link #uploadGroup}) only ever consult {@link
     * #success()} and let their own fallback logic take over on failure.
     */
    private record BulkUploadResult(boolean success, IOException failure) {
        static BulkUploadResult succeeded() {
            return new BulkUploadResult(true, null);
        }

        static BulkUploadResult failed(IOException failure) {
            return new BulkUploadResult(false, failure);
        }
    }

    /**
     * Bulk-uploads {@code localDir} to {@code containerPath} as a single tar transfer, verifying the
     * transferred file count once. Shared by the single whole-working-directory transfer in
     * {@link #uploadInputFiles} and each per-top-level directory group's bulk attempt in
     * {@link #uploadGroup} — only the retry-exhaustion/interrupt handling differs by caller, everything
     * else about a "tar a local directory, then cross-check the pod-side count" transfer is identical.
     *
     * @param label describes {@code localDir} in log/exception messages (e.g. {@code "the whole working
     *              directory"} or {@code "'data'"})
     * @return a successful {@link BulkUploadResult} if the transfer and its verification succeeded, or a
     *         failed one — carrying the triggering IOException — if the caller should fall back to a
     *         slower upload strategy instead.
     */
    private static BulkUploadResult tryBulkUploadDirectory(
        ContainerResource container,
        Logger logger,
        Path localDir,
        String containerPath,
        String label
    ) throws IOException {
        try {
            withRetries(
                logger, "uploadInputFilesBulk",
                () -> container
                    .dir(containerPath)
                    .upload(localDir)
            );
            // A genuine truncation won't self-heal across retries — every attempt re-runs the same
            // count check to the same wrong number, burning the full backoff before falling back.
            // Retrying here anyway is intentional: it's the only thing that catches the transient
            // race where 'find' runs just before the tar extraction is fully visible on the pod.
            // Accepted tradeoff — correctness for the race case over shaving a few seconds off a
            // failure path that already falls back to a slower re-upload regardless.
            // The local count is walked once, outside the retry: it's the pod-side count that's
            // racy, not the local filesystem, so re-walking it on every retry attempt is wasted work.
            var expectedFileCount = countLocalFiles(localDir);
            withVerificationRetries(logger, "verifyDirectoryUpload", () -> verifyDirectoryUpload(container, logger, containerPath, expectedFileCount));
            return BulkUploadResult.succeeded();
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                throw new IOException("Upload verification for " + label + " was interrupted", e);
            }
            logger.info("Bulk upload failed for {}, falling back to a slower upload. Reason: {}", label, e.getMessage(), e);
            // withRetries/countLocalFiles/withVerificationRetries above only ever throw IOException, but
            // the catch is intentionally broader (Exception) to also net any unchecked failure — wrap
            // that case rather than assume the cast always holds.
            var ioException = e instanceof IOException already ? already : new IOException("Bulk upload failed for " + label, e);
            return BulkUploadResult.failed(ioException);
        }
    }

    /**
     * Uploads one top-level group from {@link #uploadInputFiles}'s grouping: a directory goes through a
     * single bulk tar transfer (falling back to per-entry uploads, each individually verified, if that
     * transfer's verification fails), while a standalone file streams directly.
     */
    private static void uploadGroup(
        ContainerResource container,
        Path localBaseDir,
        String containerWorkingDir,
        Path topRelative,
        List<Path> values,
        Logger logger
    ) throws IOException {
        var topAbsolute = localBaseDir.resolve(topRelative);
        var topContainerPath = containerPath(containerWorkingDir, topRelative);

        var isBulkFallback = false;
        if (Files.isDirectory(topAbsolute)) {
            if (tryBulkUploadDirectory(container, logger, topAbsolute, topContainerPath, "'" + topRelative + "'").success()) {
                return;
            }
            isBulkFallback = true;
        }

        for (var relative : values) {
            var abs = localBaseDir.resolve(relative);
            var target = containerPath(containerWorkingDir, relative);
            // A no-op for OSS callers, whose relativePaths always resolve to regular files (the
            // top-level directory case is already handled by the bulk branch above). EE callers walk
            // the working dir into a List<Path> that can itself contain directory entries here.
            var isDirectory = Files.isDirectory(abs);

            if (isDirectory) {
                withRetries(logger, "uploadInputFiles", () -> container.dir(target).upload(abs));
            } else {
                // A fresh InputStream is opened on every attempt (not just once outside the lambda):
                // withRetries may retry, and an already-consumed stream would upload zero bytes on a
                // retried attempt. Uses upload(InputStream) — a single 'cat' exec — rather than
                // upload(Path), which tars even a lone file and requires 'tar' in the sidecar.
                withRetries(
                    logger, "uploadInputFiles",
                    () -> {
                        try (InputStream inputStream = Files.newInputStream(abs)) {
                            return container.file(target).upload(inputStream);
                        }
                    }
                );
            }

            // Only cross-check per-file uploads when this is a fallback from a failed bulk-directory
            // verification. Verifying every standalone top-level inputFile the same way would double
            // pod round-trips on the common case (many unrelated individual inputFiles), for
            // comparatively low risk since a single-file fabric8 upload is far less prone to silent
            // truncation than the tar-based bulk-directory case.
            if (isBulkFallback) {
                if (isDirectory) {
                    var expectedFileCount = countLocalFiles(abs);
                    withVerificationRetries(logger, "verifyDirectoryUpload", () -> verifyDirectoryUpload(container, logger, target, expectedFileCount));
                } else {
                    withVerificationRetries(logger, "verifyFileUpload", () -> verifyFileUpload(container, logger, target, abs));
                }
            }
        }
    }

    /**
     * Joins a POSIX container-side base path with a relative {@link Path} using '/' separators
     * regardless of the host OS — {@code Path.resolve(...).toString()} on a container path would
     * otherwise emit '\' on a Windows JVM, even though the target is always a Linux container.
     */
    private static String containerPath(String base, Path relative) {
        var sb = new StringBuilder(base);
        for (Path segment : relative) {
            sb.append('/').append(segment.toString());
        }
        return sb.toString();
    }

    /**
     * fabric8's directory upload can report success even when the tar transfer was truncated (e.g. a
     * dependency directory silently missing files), so cross-check the actual file count on the pod.
     *
     * Both sides count non-directory entries without following symlinks: tar preserves a symlink as its
     * own entry (type 'l') rather than dereferencing it, so counting only regular files locally (which
     * follows symlinks by default) would overcount against the pod-side 'find -type f' and falsely flag
     * a correct upload as truncated whenever the directory contains symlinks (e.g. a Python venv).
     *
     * The pod-side count uses {@code -print0 | tr -dc '\0' | wc -c} rather than a line-counting {@code
     * find | wc -l}: a filename containing a newline would otherwise inflate the count, since each
     * embedded newline is indistinguishable from an entry separator to 'wc -l'. NUL is the one byte
     * that cannot appear in a POSIX filename, so counting NUL bytes counts each entry exactly once.
     *
     * This is a count-only check: a truncation that drops bytes from a file's content while keeping its
     * entry (correct count, short file) is not caught here — only the single-file path ({@link
     * #verifyFileUpload}) compares byte size. Catches the reported #170 symptom (missing files), not partial
     * per-file corruption.
     *
     * @param expectedFileCount the local file count, pre-computed by {@link #countLocalFiles} once outside
     *                          any retry loop — the local filesystem can't change between retry attempts,
     *                          only the pod-side count is racy, so re-walking it on every attempt is wasted work.
     */
    private static void verifyDirectoryUpload(ContainerResource container, Logger logger, String containerPath, long expectedFileCount) throws IOException {
        verifyUpload(
            container, logger, containerPath, expectedFileCount, "file(s)",
            "find " + shellQuote(containerPath) + " ! -type d -print0 | tr -dc '\\0' | wc -c"
        );
    }

    /**
     * Counts the non-directory entries under {@code localPath} without following symlinks — see {@link
     * #verifyDirectoryUpload} for why symlinks are counted as their own entry rather than dereferenced.
     */
    private static long countLocalFiles(Path localPath) throws IOException {
        try (var files = Files.walk(localPath)) {
            return files.filter(path -> !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)).count();
        }
    }

    /**
     * Single-file counterpart of {@link #verifyDirectoryUpload}, comparing the uploaded file's size on
     * the pod against the local file to catch a partial/corrupt transfer that fabric8 still reports as success.
     * <p>
     * Uses an explicit if/else rather than {@code test -e ... && wc -c ... || echo 0}: with the latter, a
     * missing 'wc' (tool absent, file present) would make the {@code &&} short-circuit fall through to the
     * {@code ||} branch and report 0, indistinguishable from a genuinely missing file. The if/else form keeps
     * those two cases separate — a missing 'wc' still makes the whole command exit non-zero (check skipped,
     * as before).
     * <p>
     * The missing-file branch reports {@code -1}, not {@code 0}: a genuinely present but empty local file
     * has an expected byte count of 0, and {@code actual < expected} with both sides 0 would never flag a
     * missing pod-side file as a shortfall. A negative sentinel is always below any real expected byte
     * count (including 0), so it reliably fails the comparison while a present empty file (which reports
     * 0 via {@code wc -c}) still passes.
     */
    private static void verifyFileUpload(ContainerResource container, Logger logger, String containerPath, Path localFile) throws IOException {
        var quotedPath = shellQuote(containerPath);
        verifyUpload(
            container, logger, containerPath, Files.size(localFile), "byte(s)",
            "if [ -e " + quotedPath + " ]; then wc -c < " + quotedPath + "; else echo -1; fi"
        );
    }

    /**
     * Single-quotes a value for safe interpolation into a `sh -c` command, escaping any embedded quote.
     */
    private static String shellQuote(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    /**
     * Best-effort: if the sidecar image lacks 'find'/'wc', the check is skipped rather than failing the task.
     */
    private static void verifyUpload(ContainerResource container, Logger logger, String containerPath, long expected, String unit, String shellCommand) throws IOException {
        var actual = execOutput(container, logger, UPLOAD_VERIFICATION_TIMEOUT, "sh", "-c", shellCommand)
            .map(String::trim)
            .flatMap(PodService::parseLong);

        if (actual.isEmpty()) {
            logger.debug("Skipping upload verification for '{}': the file-sidecar image does not support this check, or the verification command timed out", containerPath);
            return;
        }

        // Only a shortfall means truncation — a transfer cannot add entries. A negative actual is the
        // missing-file sentinel from verifyFileUpload's else branch (see its Javadoc), not a real count.
        if (actual.get() < 0) {
            throw new IOException(
                "Upload verification failed for '" + containerPath + "': the file is missing in the file-sidecar container"
            );
        }
        if (actual.get() < expected) {
            throw new IOException(
                "Upload verification failed for '" + containerPath + "': expected at least " + expected + " " + unit + " but found " +
                    actual.get() + " in the file-sidecar container — the tar transfer was likely truncated"
            );
        }
    }

    private static Optional<Long> parseLong(String output) {
        try {
            return Optional.of(Long.parseLong(output));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static void uploadMarker(RunContext runContext, PodResource podResource, Logger logger, String marker, String container) throws IOException {
        File markerFile = tempDir(runContext).resolve(marker).toFile();
        // The working directory can be reused across multiple run() calls sharing the same RunContext,
        // so clear out any stale marker left behind by a previous run whose cleanup delete failed.
        Files.deleteIfExists(markerFile.toPath());
        if (!markerFile.createNewFile()) {
            throw new IOException("Unable to create the marker file: " + markerFile.getAbsolutePath());
        }

        withRetries(
            logger,
            "uploadMarker",
            () -> podResource
                .inContainer(container)
                .withReadyWaitTimeout(0)
                .file("/kestra/" + marker)
                .upload(markerFile.toPath())
        );

        if (!markerFile.delete()) {
            logger.debug("Unable to delete the marker file: {}", markerFile.getAbsolutePath());
        }
        logger.debug(marker + " marker uploaded");
    }

    /**
     * Fetch and log Kubernetes pod events (e.g. FailedScheduling, Evicted, ImagePullBackOff).
     */
    public static void logPodEvents(KubernetesClient client, Pod pod, Logger logger, AbstractLogConsumer logConsumer) {
        if (pod == null || pod.getMetadata() == null) {
            return;
        }

        String namespace = pod.getMetadata().getNamespace();
        String podName = pod.getMetadata().getName();

        try {
            client.v1().events()
                .inNamespace(namespace)
                .withField("involvedObject.name", podName)
                .list()
                .getItems()
                .stream()
                .filter(event -> "Warning".equals(event.getType()))
                .sorted(
                    Comparator.comparing(
                        Event::getLastTimestamp,
                        Comparator.nullsLast(Comparator.naturalOrder())
                    )
                )
                .forEach(event ->
                {
                    String reason = event.getReason() == null ? "" : event.getReason();
                    String message = event.getMessage() == null ? "" : event.getMessage();

                    logConsumer.accept("[pod-event] " + reason + " - " + message, true);
                });

        } catch (Exception e) {
            logger.warn("Failed to fetch events for pod '{}'", podName, e);
        }
    }

    public static boolean hasAnyContainerStarted(Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return false;
        }
        return pod.getStatus().getContainerStatuses().stream()
            .anyMatch(
                cs -> cs.getState() != null &&
                    (cs.getState().getRunning() != null || cs.getState().getTerminated() != null)
            );
    }

    public static boolean hasNonTransientWaitingContainer(Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getContainerStatuses() == null) {
            return false;
        }
        return pod.getStatus().getContainerStatuses().stream()
            .anyMatch(
                cs -> cs.getState() != null &&
                    cs.getState().getWaiting() != null &&
                    cs.getState().getWaiting().getReason() != null &&
                    !TransientWaitingReason.contains(cs.getState().getWaiting().getReason())
            );
    }

    public static Path tempDir(RunContext runContext) {
        return runContext.workingDir().path().resolve("working-dir");
    }

    /**
     * Moves a file, creating the destination's parent directories if needed.
     */
    public static void moveFile(Path from, Path to) throws IOException {
        if (Files.notExists(to.getParent())) {
            Files.createDirectories(to.getParent());
        }
        Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Maps a sidecar's rendered resources block to Kubernetes {@link ResourceRequirements}.
     *
     * @return the resource requirements, or null when the sidecar defines none
     */
    @SuppressWarnings("unchecked")
    public static ResourceRequirements mapSidecarResources(RunContext runContext, SideCar sideCar) throws IllegalVariableEvaluationException {
        if (sideCar == null) {
            return null;
        }

        Map<String, Object> sidecarResources = runContext.render(sideCar.getResources()).asMap(String.class, Object.class);
        if (sidecarResources == null) {
            return null;
        }

        ResourceRequirementsBuilder resourceRequirementsBuilder = new ResourceRequirementsBuilder();
        if (sidecarResources.containsKey("claims")) {
            try {
                resourceRequirementsBuilder.withClaims((List<ResourceClaim>) sidecarResources.get("claims"));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Sidecar resources claims must be a list of resource claims");
            }
        }
        if (sidecarResources.containsKey("limits")) {
            try {
                resourceRequirementsBuilder.withLimits((Map<String, Quantity>) sidecarResources.get("limits"));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Sidecar resources limits must be a map of string to quantity");
            }
        }
        if (sidecarResources.containsKey("requests")) {
            try {
                resourceRequirementsBuilder.withRequests((Map<String, Quantity>) sidecarResources.get("requests"));
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Sidecar resources requests must be a map of string to quantity");
            }
        }
        return resourceRequirementsBuilder.build();
    }

    public enum TransientWaitingReason {
        CONTAINER_CREATING("ContainerCreating"),
        POD_INITIALIZING("PodInitializing");

        private final String reason;

        TransientWaitingReason(String reason) {
            this.reason = reason;
        }

        private static final Set<String> REASONS = Arrays.stream(values())
            .map(r -> r.reason)
            .collect(Collectors.toUnmodifiableSet());

        public static boolean contains(String reason) {
            return REASONS.contains(reason);
        }
    }

    public enum PodPhase {
        PENDING("Pending"),
        RUNNING("Running"),
        SUCCEEDED("Succeeded"),
        FAILED("Failed"),
        UNKNOWN("Unknown");

        private final String value;

        PodPhase(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }
}
