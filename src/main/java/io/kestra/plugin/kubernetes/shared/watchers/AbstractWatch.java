package io.kestra.plugin.kubernetes.shared.watchers;

import org.slf4j.Logger;

import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.WatcherException;

abstract public class AbstractWatch<T> implements io.fabric8.kubernetes.client.Watcher<T> {
    private static final int HTTP_GONE = 410;

    protected Logger logger;

    public AbstractWatch(Logger logger) {
        this.logger = logger;
    }

    public void eventReceived(Action action, T resource) {
        logger.debug("Received action '{}' on [{}]", action, this.logContext(resource));
    }

    public void onClose() {
        logger.debug("Received close on [Type: {}]", this.getClass().getSimpleName());
    }

    /**
     * A closed watch never fails the task: watches are diagnostic only here, every wait and completion
     * decision is taken by polling in PodService. So a close is reported below ERROR level — the logger
     * is the user-facing runContext one, and an ERROR entry there misrepresents an execution that
     * succeeded as one that failed. It can even carry no text at all: on the HTTP 410 path fabric8
     * builds the exception from the API server Status, whose message is nullable.
     */
    public void onClose(WatcherException e) {
        if (e == null) {
            logger.debug("Received close on [Type: {}]", this.getClass().getSimpleName());
            return;
        }

        if (isRoutineClose(e)) {
            logger.debug(
                "Watch on [Type: {}] was closed: {}",
                this.getClass().getSimpleName(),
                describe(e)
            );
            return;
        }

        logger.warn(
            "Watch on [Type: {}] closed unexpectedly: {}",
            this.getClass().getSimpleName(),
            describe(e),
            e
        );
    }

    /**
     * Two closes are routine, and neither says anything about the task. An HTTP 410 Gone: the API server
     * ends watches whose resourceVersion went stale, and fabric8 also synthesizes a 410 when a watch ends
     * cleanly without ever delivering a message. And exhausted reconnects, which fabric8 reports with no
     * cause attached — the client stopped retrying this watch, while the polling that actually drives the
     * task carries on. What is left always carries a cause and is a client-side malfunction: an unhandled
     * exception thrown during a reconnect attempt, or a watch response that could not be parsed.
     */
    private static boolean isRoutineClose(WatcherException e) {
        return e.getCause() == null || hasHttpGoneCause(e);
    }

    /**
     * Deliberately not {@link WatcherException#isHttpGone()}: that reads {@code Status.getCode()} as an
     * {@code int} on the non-410 branch, so a cause carrying a Status with no code would throw from this
     * callback. The cause we get on the 410 paths is always a {@link KubernetesClientException}.
     */
    private static boolean hasHttpGoneCause(WatcherException e) {
        return e.getCause() instanceof KubernetesClientException clientException
            && clientException.getCode() == HTTP_GONE;
    }

    /**
     * A non-blank description of the close, since the exception message is null on the 410 paths — where
     * the cause is a KubernetesClientException with no message of its own either, so falling back to
     * {@link Throwable#toString()} would print a bare class name. Describe the API server Status instead.
     */
    private static String describe(WatcherException e) {
        if (e.getMessage() != null && !e.getMessage().isBlank()) {
            return e.getMessage();
        }

        return e.getRawWatchMessage()
            .filter(rawWatchMessage -> !rawWatchMessage.isBlank())
            .orElseGet(() -> describeCause(e.getCause()));
    }

    private static String describeCause(Throwable cause) {
        if (cause == null) {
            return "no message";
        }

        if (cause instanceof KubernetesClientException clientException && cause.getMessage() == null) {
            Status status = clientException.getStatus();

            if (status != null) {
                return "code " + clientException.getCode()
                    + (status.getReason() == null ? "" : ", reason " + status.getReason());
            }
        }

        return cause.toString();
    }

    abstract protected String logContext(T resource);
}
