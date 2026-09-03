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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The logger handed to a watch is the user-facing runContext one, so a closed watch must never be
 * logged at ERROR: watches are diagnostic only, and an ERROR entry on an otherwise successful
 * execution shows up as a bare "Error" label in the UI (kestra-ee#8204).
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
     * Status carries no message, so the exception message is null.
     */
    private static WatcherException httpGoneWithoutMessage() {
        return new WatcherException(
            null,
            new KubernetesClientException(new StatusBuilder().withCode(410).withReason("Expired").build())
        );
    }

    @Test
    void httpGoneCloseIsNotUserFacing() {
        Logger logger = mock(Logger.class);

        new TestWatch(logger).onClose(httpGoneWithoutMessage());

        verify(logger, never()).error(any(), any(Object[].class));
        verify(logger, never()).error(anyString(), any(Throwable.class));
        verify(logger, never()).warn(anyString(), any(), any());
    }

    @Test
    void unexpectedCloseIsWarnedWithItsMessage() {
        Logger logger = mock(Logger.class);
        WatcherException exception = new WatcherException("Exhausted reconnects");

        new TestWatch(logger).onClose(exception);

        ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
        verify(logger).warn(anyString(), any(), description.capture(), any());
        assertThat(description.getValue().toString(), containsString("Exhausted reconnects"));

        verify(logger, never()).error(any(), any(Object[].class));
        verify(logger, never()).error(anyString(), any(Throwable.class));
    }

    @Test
    void closeWithoutAnyMessageStillDescribesItsCause() {
        Logger logger = mock(Logger.class);

        new TestWatch(logger).onClose(new WatcherException(null, new IllegalStateException("socket died")));

        ArgumentCaptor<Object> description = ArgumentCaptor.forClass(Object.class);
        verify(logger).warn(anyString(), any(), description.capture(), any());
        assertThat(description.getValue().toString(), containsString("socket died"));
        assertThat(description.getValue().toString(), not(containsString("null")));
    }

    @Test
    void nullCloseExceptionIsIgnored() {
        Logger logger = mock(Logger.class);

        new TestWatch(logger).onClose((WatcherException) null);

        verify(logger, never()).error(any(), any(Object[].class));
        verify(logger, never()).error(anyString(), any(Throwable.class));
        verify(logger, never()).warn(anyString(), any(), any(), any());
    }
}
