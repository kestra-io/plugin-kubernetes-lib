package io.kestra.plugin.kubernetes.shared.models;

import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(
    title = "OAuth token provider",
    description = """
        Dynamically refreshes an OAuth token by executing a Kestra task each time the Kubernetes client
        needs to authenticate. This is essential for cloud providers (e.g. GCP, AWS, Azure) whose tokens
        are short-lived (typically 1 hour). Without automatic refresh, long-running tasks will fail with
        an unauthorized error once the initial token expires."""
)
public class OAuthTokenProvider implements io.fabric8.kubernetes.client.OAuthTokenProvider {
    @Schema(
        title = "Token provider task",
        description = """
            A Kestra task that fetches a fresh OAuth token. This task is executed each time the
            Kubernetes client needs to authenticate, enabling automatic token refresh for long-running
            operations."""
    )
    @PluginProperty(group = "advanced")
    private Task task;

    @Schema(
        title = "Token output expression",
        description = """
            An expression to extract the token value from the task output,
            e.g. `{{ accessToken.tokenValue }}`."""
    )
    @PluginProperty(group = "advanced")
    private String output;

    @Schema(
        title = "Token cache duration",
        description = """
            How long a fetched token is reused before the provider executes the task again.
            Caching avoids hammering the token endpoint on every Kubernetes API call.
            Defaults to 5 minutes (`PT5M`). Set to `PT0S` to disable caching."""
    )
    @PluginProperty(group = "advanced")
    @Builder.Default
    private Duration cache = Duration.ofMinutes(5);

    @With
    private transient RunContext runContext;

    // Cached token state — volatile so that visibility is guaranteed across threads.
    // getToken() is synchronized so only one thread refreshes at a time.
    @JsonIgnore
    private transient volatile String cachedToken;

    @JsonIgnore
    private transient volatile Instant cacheExpiresAt;

    @Override
    @JsonIgnore
    public synchronized String getToken() {
        var now = Instant.now();
        if (cachedToken != null && cacheExpiresAt != null && now.isBefore(cacheExpiresAt)) {
            return cachedToken;
        }

        try {
            RunnableTask<?> runnableTask = (RunnableTask<?>) task;
            Output run = runnableTask.run(runContext);
            var token = runContext.render(this.output, run.toMap());

            // Only cache when a positive duration is configured.
            if (cache != null && !cache.isZero() && !cache.isNegative()) {
                cachedToken = token;
                cacheExpiresAt = now.plus(cache);
            }

            return token;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
