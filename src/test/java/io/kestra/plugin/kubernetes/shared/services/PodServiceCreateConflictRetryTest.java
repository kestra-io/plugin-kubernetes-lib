package io.kestra.plugin.kubernetes.shared.services;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.api.model.StatusDetailsBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;

class PodServiceCreateConflictRetryTest {
    private static final Duration DELAY = Duration.ofMillis(1);

    private final Logger logger = mock(Logger.class);

    private static KubernetesClientException exception(int code, String reason) {
        var status = new StatusBuilder()
            .withCode(code)
            .withReason(reason)
            .withMessage("boom")
            .withDetails(new StatusDetailsBuilder().withKind("resourcequotas").withName("default").build())
            .build();
        return new KubernetesClientException("boom", code, status);
    }

    private static Supplier<String> failing(AtomicInteger calls, int failures, KubernetesClientException e) {
        return () -> {
            if (calls.incrementAndGet() <= failures) {
                throw e;
            }
            return "pod";
        };
    }

    private String run(Supplier<String> create) {
        return PodService.createWithConflictRetry(logger, "pod", create, DELAY, DELAY);
    }

    @Test
    void succeedsFirstTry() {
        var calls = new AtomicInteger();

        assertThat(run(failing(calls, 0, exception(409, "Conflict"))), is("pod"));
        assertThat(calls.get(), is(1));
    }

    @Test
    void retriesConflictThenSucceeds() {
        var calls = new AtomicInteger();

        assertThat(run(failing(calls, 2, exception(409, "Conflict"))), is("pod"));
        assertThat(calls.get(), is(3));
        verify(logger, times(2)).warn(anyString(), eq("pod"), eq(" (conflicting resource: resourcequotas/default)"), anyInt(), eq(5), anyLong(), any());
    }

    @Test
    void rethrowsOriginalAfterFiveAttempts() {
        var calls = new AtomicInteger();
        var last = exception(409, "Conflict");

        var thrown = assertThrows(KubernetesClientException.class, () -> run(failing(calls, Integer.MAX_VALUE, last)));

        assertThat(thrown, is(sameInstance(last)));
        assertThat(calls.get(), is(5));
    }

    @Test
    void doesNotRetryAlreadyExists() {
        var calls = new AtomicInteger();

        assertThrows(KubernetesClientException.class, () -> run(failing(calls, Integer.MAX_VALUE, exception(409, "AlreadyExists"))));
        assertThat(calls.get(), is(1));
    }

    @Test
    void doesNotRetryOtherCodes() {
        for (var code : new int[]{403, 422}) {
            var calls = new AtomicInteger();

            assertThrows(KubernetesClientException.class, () -> run(failing(calls, Integer.MAX_VALUE, exception(code, "Forbidden"))));
            assertThat(calls.get(), is(1));
        }
    }

    @Test
    void doesNotRetryConflictWithoutStatus() {
        var calls = new AtomicInteger();

        assertThrows(KubernetesClientException.class, () -> run(failing(calls, Integer.MAX_VALUE, new KubernetesClientException("boom", 409, null))));
        assertThat(calls.get(), is(1));
    }

    @Test
    void interruptDuringBackoffStopsRetries() {
        var calls = new AtomicInteger();
        var last = exception(409, "Conflict");
        Thread.currentThread().interrupt();

        try {
            var thrown = assertThrows(
                KubernetesClientException.class,
                () -> PodService.createWithConflictRetry(logger, "pod", failing(calls, Integer.MAX_VALUE, last), Duration.ofSeconds(1), Duration.ofSeconds(1))
            );

            assertThat(thrown, is(sameInstance(last)));
            assertThat(calls.get(), is(1));
            assertThat(Thread.currentThread().isInterrupted(), is(true));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void backoffGrowsExponentiallyWithJitterAndCap() {
        var calls = new AtomicInteger();

        assertThrows(
            KubernetesClientException.class,
            () -> PodService.createWithConflictRetry(
                logger, "pod", failing(calls, Integer.MAX_VALUE, exception(409, "Conflict")), Duration.ofMillis(8), Duration.ofMillis(20)
            )
        );

        var delays = mockingDetails(logger).getInvocations().stream()
            .filter(invocation -> invocation.getMethod().getName().equals("warn"))
            .map(invocation -> (Long) invocation.getArguments()[5])
            .toList();
        var ceilings = new long[]{8, 16, 20, 20};

        assertThat(delays.size(), is(ceilings.length));
        for (var i = 0; i < ceilings.length; i++) {
            assertThat(delays.get(i), is(both(greaterThanOrEqualTo(ceilings[i] / 2)).and(lessThanOrEqualTo(ceilings[i]))));
        }
    }
}
