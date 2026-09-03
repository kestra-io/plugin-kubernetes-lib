package io.kestra.plugin.kubernetes.shared.watchers;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.WatcherException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * The logger handed to a watch is the user-facing runContext one, so a closed watch must never be
 * logged at ERROR: watches are diagnostic only, and an ERROR entry on an otherwise successful
 * execution shows up as a bare "Error" label in the UI (kestra-ee#8204).
 * <p>
 * Every test closes with {@link org.mockito.Mockito#verifyNoMoreInteractions} rather than
 * {@code never()} on individual overloads: {@code Logger} has one method per arity, so a
 * {@code never()} check pins the overload it happens to resolve to and would stay green if a
 * regression logged the error through a different one.
 */
class AbstractWatchTest {
    private static class TestWatch extends AbstractWatch<String> {
        TestWatch(Logger logger) {
            super(logger);
        }

        @Override
        protected String logContext(String resource) {
            return resource;
        }
    }

    /**
     * How fabric8 reports a watch the API server ended because the resourceVersion went stale: the
     * Status carries no message, so both the exception message and the cause's message are null.
     */
    private static WatcherException httpGoneWithoutMessage() {
        return new WatcherException(
            null,
            new KubernetesClientException(new StatusBuilder().withCode(410).withReason("Expired").build())
        );
    }

    @Test
    void httpGoneCloseIsOnlyDebugged() {
        Logger logger = mock(Logger.class);

        new TestWatch(logger).onClose(httpGoneWithoutMessage());

        ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
        verify(logger).debug(anyString(), any(), description.capture());
        verifyNoMoreInteractions(logger);

        // A bare Throwable.toString() would only print the exception class name here.
        assertThat(description.getValue().toString(), containsString("code 410"));
        assertThat(description.getValue().toString(), containsString("Expired"));
    }

    @Test
    void unexpectedCloseIsWarnedWithItsMessage() {
        Logger logger = mock(Logger.class);
        WatcherException exception = new WatcherException("Exhausted reconnects");

        new TestWatch(logger).onClose(exception);

        ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
        verify(logger).warn(anyString(), any(), description.capture(), any());
        verifyNoMoreInteractions(logger);

        assertThat(description.getValue().toString(), containsString("Exhausted reconnects"));
    }

    @Test
    void closeWithoutAnyMessageStillDescribesItsCause() {
        Logger logger = mock(Logger.class);

        new TestWatch(logger).onClose(new WatcherException(null, new IllegalStateException("socket died")));

        ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
        verify(logger).warn(anyString(), any(), description.capture(), any());
        verifyNoMoreInteractions(logger);

        assertThat(description.getValue().toString(), containsString("socket died"));
        assertThat(description.getValue().toString(), not(containsString("null")));
    }

    @Test
    void nullCloseExceptionIsOnlyDebugged() {
        Logger logger = mock(Logger.class);

        new TestWatch(logger).onClose((WatcherException) null);

        verify(logger).debug(anyString(), any(Object.class));
        verifyNoMoreInteractions(logger);
    }
}
