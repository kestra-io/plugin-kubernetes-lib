package io.kestra.plugin.kubernetes.shared.models;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OAuthTokenProviderTest {

    /**
     * Minimal abstract stub that is both a Task and a RunnableTask, allowing Mockito to mock it.
     * Mockito will generate a concrete subclass of this abstract class.
     */
    abstract static class TokenTask extends Task implements RunnableTask<Output> {
    }

    @Mock
    private TokenTask tokenTask;

    @Mock
    private RunContext runContext;

    @Mock
    private Output taskOutput;

    @BeforeEach
    void setUp() throws Exception {
        when(taskOutput.toMap()).thenReturn(Map.of("accessToken", Map.of("tokenValue", "tok")));
        when(tokenTask.run(runContext)).thenReturn(taskOutput);
        // Default render result; individual tests may override this stub as needed.
        when(runContext.render(eq("{{ accessToken.tokenValue }}"), any())).thenReturn("token-v1");
    }

    // --- happy path ---

    @Test
    void shouldFetchTokenOnFirstCall() throws Exception {
        var provider = OAuthTokenProvider.builder()
            .task(tokenTask)
            .output("{{ accessToken.tokenValue }}")
            .cache(Duration.ofMinutes(5))
            .runContext(runContext)
            .build();

        var token = provider.getToken();

        assertThat(token, is("token-v1"));
        verify(tokenTask, times(1)).run(runContext);
    }

    // --- cache hit ---

    @Test
    void shouldReturnCachedTokenWithinTtl() throws Exception {
        var provider = OAuthTokenProvider.builder()
            .task(tokenTask)
            .output("{{ accessToken.tokenValue }}")
            .cache(Duration.ofMinutes(5))
            .runContext(runContext)
            .build();

        // Two consecutive calls within the TTL
        var first = provider.getToken();
        var second = provider.getToken();

        assertThat(first, is("token-v1"));
        assertThat(second, is("token-v1"));
        // The underlying task must only be executed once
        verify(tokenTask, times(1)).run(runContext);
    }

    // --- cache miss after TTL ---

    @Test
    void shouldRefetchTokenAfterTtlExpires() throws Exception {
        when(runContext.render(eq("{{ accessToken.tokenValue }}"), any()))
            .thenReturn("token-v1")
            .thenReturn("token-v2");

        // Use a zero-duration cache so every call is treated as expired
        var provider = OAuthTokenProvider.builder()
            .task(tokenTask)
            .output("{{ accessToken.tokenValue }}")
            .cache(Duration.ZERO)
            .runContext(runContext)
            .build();

        var first = provider.getToken();
        var second = provider.getToken();

        assertThat(first, is("token-v1"));
        assertThat(second, is("token-v2"));
        // Task must be executed on every call since caching is disabled
        verify(tokenTask, times(2)).run(runContext);
    }

    // --- cache miss after a positive TTL genuinely elapses ---

    @Test
    void shouldRefetchTokenAfterPositiveTtlElapses() throws Exception {
        when(runContext.render(eq("{{ accessToken.tokenValue }}"), any()))
            .thenReturn("token-v1")
            .thenReturn("token-v2");

        // Short positive TTL so the cached token expires over real time,
        // exercising the cacheExpiresAt comparison rather than the caching-disabled branch.
        var provider = OAuthTokenProvider.builder()
            .task(tokenTask)
            .output("{{ accessToken.tokenValue }}")
            .cache(Duration.ofMillis(100))
            .runContext(runContext)
            .build();

        var first = provider.getToken();
        Thread.sleep(200);
        var second = provider.getToken();

        assertThat(first, is("token-v1"));
        assertThat(second, is("token-v2"));
        // Task must run again once the cached token has expired
        verify(tokenTask, times(2)).run(runContext);
    }

    // --- default TTL ---

    @Test
    void defaultCacheDurationIsFiveMinutes() {
        var provider = OAuthTokenProvider.builder()
            .task(tokenTask)
            .output("{{ accessToken.tokenValue }}")
            .runContext(runContext)
            .build();

        assertThat(provider.getCache(), is(Duration.ofMinutes(5)));
    }

    // --- negative duration disables caching ---

    @Test
    void negativeCacheDurationDisablesCaching() throws Exception {
        when(runContext.render(eq("{{ accessToken.tokenValue }}"), any()))
            .thenReturn("token-v1")
            .thenReturn("token-v2");

        var provider = OAuthTokenProvider.builder()
            .task(tokenTask)
            .output("{{ accessToken.tokenValue }}")
            .cache(Duration.ofSeconds(-1))
            .runContext(runContext)
            .build();

        provider.getToken();
        provider.getToken();

        verify(tokenTask, times(2)).run(runContext);
    }
}
