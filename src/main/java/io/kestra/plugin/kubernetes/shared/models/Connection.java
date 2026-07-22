package io.kestra.plugin.kubernetes.shared.models;

import java.time.Duration;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Connection {
    @Schema(
        title = "Trust all certificates",
        description = "When true, skips TLS cert validation. Use only for testing."
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> trustCerts;

    @Schema(
        title = "Disable hostname verification",
        description = "Disables TLS hostname checks. Avoid in production clusters."
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> disableHostnameVerification;

    @Schema(
        title = "Kubernetes API URL",
        description = "API server endpoint. Default `https://kubernetes.default.svc`."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private final Property<String> masterUrl = Property.ofValue("https://kubernetes.default.svc");

    @Schema(
        title = "API version",
        description = "API group version used by the client. Default v1."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private final Property<String> apiVersion = Property.ofValue("v1");

    @Schema(
        title = "Default namespace",
        description = "Namespace used when resources omit a namespace."
    )
    @PluginProperty(group = "connection")
    private final Property<String> namespace;

    @Schema(
        title = "CA certificate file",
        description = "Path to a PEM CA bundle."
    )
    @PluginProperty(group = "advanced")
    private final Property<String> caCertFile;

    @Schema(
        title = "CA certificate data",
        description = "Base64-encoded PEM CA bundle. Whitespace is stripped automatically."
    )
    @PluginProperty(group = "advanced")
    private final Property<String> caCertData;

    @Schema(
        title = "Client certificate file"
    )
    @PluginProperty(group = "advanced")
    private final Property<String> clientCertFile;

    @Schema(
        title = "Client certificate data",
        description = "Base64-encoded client cert. Whitespace is stripped automatically."
    )
    @PluginProperty(group = "advanced")
    private final Property<String> clientCertData;

    @Schema(
        title = "Client key file"
    )
    @PluginProperty(group = "advanced")
    private final Property<String> clientKeyFile;

    @Schema(
        title = "Client key data",
        description = "Base64-encoded client key. Whitespace is stripped automatically."
    )
    @PluginProperty(secret = true, group = "advanced")
    private final Property<String> clientKeyData;

    @Schema(
        title = "Client key algorithm",
        description = "Algorithm for the client key. Default RSA."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private final Property<String> clientKeyAlgo = Property.ofValue("RSA");

    @Schema(
        title = "Client key passphrase"
    )
    @PluginProperty(secret = true, group = "advanced")
    private final Property<String> clientKeyPassphrase;

    @Schema(
        title = "Truststore file"
    )
    @PluginProperty(group = "advanced")
    private final Property<String> trustStoreFile;

    @Schema(
        title = "Truststore passphrase"
    )
    @PluginProperty(secret = true, group = "advanced")
    private final Property<String> trustStorePassphrase;

    @Schema(
        title = "Keystore file"
    )
    @PluginProperty(group = "advanced")
    private final Property<String> keyStoreFile;

    @Schema(
        title = "Keystore passphrase"
    )
    @PluginProperty(secret = true, group = "advanced")
    private final Property<String> keyStorePassphrase;

    @Schema(
        title = "OAuth token"
    )
    @PluginProperty(secret = true, group = "connection")
    private final Property<String> oauthToken;

    @Schema(
        title = "OAuth token provider",
        description = """
            Dynamically refreshes the OAuth token used to authenticate with the Kubernetes API.
            Use this when authenticating via a cloud provider (e.g. GCP, AWS, Azure) whose tokens are
            short-lived (typically 1 hour). Without this, long-running tasks will fail with an unauthorized
            error once the initial token expires. The provider executes a Kestra task to fetch a fresh token
            each time the Kubernetes client needs to re-authenticate."""
    )
    @PluginProperty(secret = true, group = "connection")
    private final OAuthTokenProvider oauthTokenProvider;

    @Schema(
        title = "Username"
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> username;

    @Schema(
        title = "Password"
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> password;

    @Schema(
        title = "Max concurrent HTTP requests",
        description = """
            Caps total concurrent HTTP requests issued by this client. Lower this when running many
            concurrent tasks against a rate-limited API server (e.g. GKE). Fabric8 default: 64."""
    )
    @PluginProperty(group = "advanced")
    private final Property<Integer> maxConcurrentRequests;

    @Schema(
        title = "Max concurrent HTTP requests per host",
        description = "Caps concurrent HTTP requests to a single host (the API server). Fabric8 default: 5."
    )
    @PluginProperty(group = "advanced")
    private final Property<Integer> maxConcurrentRequestsPerHost;

    @Schema(
        title = "Watch reconnect interval",
        description = """
            Backoff between watch reconnect attempts. Increase to prevent reconnect storms under
            API server pressure. Fabric8 default: 1s."""
    )
    @PluginProperty(group = "advanced")
    private final Property<Duration> watchReconnectInterval;

    @Schema(
        title = "Enable HTTP/2",
        description = """
            Off by default (HTTP/1.1, the prior behavior). Enable to use HTTP/2, which keeps
            long-lived watch/log streams alive better through proxies like GKE konnectivity."""
    )
    @PluginProperty(group = "advanced")
    private final Property<Boolean> enableHttp2;

    @Schema(
        title = "WebSocket ping interval",
        description = """
            Keepalive ping on exec/log connections so idle streams aren't dropped by a proxy
            (e.g. GKE konnectivity). Only honored by the OkHttp backend. 0 disables."""
    )
    @PluginProperty(group = "advanced")
    private final Property<Duration> websocketPingInterval;

    public Config toConfig(RunContext runContext) throws IllegalVariableEvaluationException {
        return toConfig(runContext, false);
    }

    public Config toConfig(RunContext runContext, boolean inheritClusterConfig) throws IllegalVariableEvaluationException {
        // inheritClusterConfig seeds the base from ambient auto-config (system props / env / kubeconfig /
        // in-cluster SA) instead of blank, so explicit fields override while the rest stays resolved.
        ConfigBuilder builder = new ConfigBuilder(inheritClusterConfig ? Config.autoConfigure(null) : Config.empty());

        if (trustCerts != null) {
            boolean trustCertsValue = runContext.render(trustCerts).as(Boolean.class).orElseThrow();
            if (trustCertsValue) {
                runContext.logger().warn(
                    "TLS certificate validation is disabled (trustCerts=true) for this Kubernetes connection. " +
                        "This bypasses certificate trust checks and should never be used against production clusters."
                );
            }
            builder.withTrustCerts(trustCertsValue);
        }

        if (disableHostnameVerification != null) {
            boolean disableHostnameVerificationValue = runContext.render(disableHostnameVerification).as(Boolean.class).orElseThrow();
            if (disableHostnameVerificationValue) {
                runContext.logger().warn(
                    "TLS hostname verification is disabled (disableHostnameVerification=true) for this Kubernetes connection. " +
                        "This exposes the connection to man-in-the-middle attacks and should never be used against production clusters."
                );
            }
            builder.withDisableHostnameVerification(disableHostnameVerificationValue);
        }

        if (masterUrl != null) {
            builder.withMasterUrl(runContext.render(masterUrl).as(String.class).orElseThrow());
        }

        if (apiVersion != null) {
            builder.withApiVersion(runContext.render(apiVersion).as(String.class).orElseThrow());
        }

        if (namespace != null) {
            builder.withNamespace(runContext.render(namespace).as(String.class).orElseThrow());
        }

        if (caCertFile != null) {
            builder.withCaCertFile(runContext.render(caCertFile).as(String.class).orElseThrow());
        }

        if (caCertData != null) {
            builder.withCaCertData(normalizeBase64(runContext, caCertData));
        }

        if (clientCertFile != null) {
            builder.withClientCertFile(runContext.render(clientCertFile).as(String.class).orElseThrow());
        }

        if (oauthToken != null) {
            builder.withOauthToken(runContext.render(oauthToken).as(String.class).orElseThrow());
        }

        if (oauthTokenProvider != null) {
            builder.withOauthTokenProvider(oauthTokenProvider.withRunContext(runContext));
        }

        if (clientCertData != null) {
            builder.withClientCertData(normalizeBase64(runContext, clientCertData));
        }

        if (clientKeyFile != null) {
            builder.withClientKeyFile(runContext.render(clientKeyFile).as(String.class).orElseThrow());
        }

        if (clientKeyData != null) {
            builder.withClientKeyData(normalizeBase64(runContext, clientKeyData));
        }

        if (clientKeyAlgo != null) {
            builder.withClientKeyAlgo(runContext.render(clientKeyAlgo).as(String.class).orElseThrow());
        }

        if (clientKeyPassphrase != null) {
            builder.withClientKeyPassphrase(runContext.render(clientKeyPassphrase).as(String.class).orElseThrow());
        }

        if (trustStoreFile != null) {
            builder.withTrustStoreFile(runContext.render(trustStoreFile).as(String.class).orElseThrow());
        }

        if (trustStorePassphrase != null) {
            builder.withTrustStorePassphrase(runContext.render(trustStorePassphrase).as(String.class).orElseThrow());
        }

        if (keyStoreFile != null) {
            builder.withKeyStoreFile(runContext.render(keyStoreFile).as(String.class).orElseThrow());
        }

        if (keyStorePassphrase != null) {
            builder.withKeyStorePassphrase(runContext.render(keyStorePassphrase).as(String.class).orElseThrow());
        }

        if (username != null) {
            builder.withUsername(runContext.render(username).as(String.class).orElseThrow());
        }

        if (password != null) {
            builder.withPassword(runContext.render(password).as(String.class).orElseThrow());
        }

        if (maxConcurrentRequests != null) {
            builder.withMaxConcurrentRequests(runContext.render(maxConcurrentRequests).as(Integer.class).orElseThrow());
        }

        if (maxConcurrentRequestsPerHost != null) {
            builder.withMaxConcurrentRequestsPerHost(runContext.render(maxConcurrentRequestsPerHost).as(Integer.class).orElseThrow());
        }

        if (watchReconnectInterval != null) {
            builder.withWatchReconnectInterval((int) runContext.render(watchReconnectInterval).as(Duration.class).orElseThrow().toMillis());
        }

        // Default to HTTP/1.1 (previous behavior); HTTP/2 is opt-in.
        boolean http2 = enableHttp2 != null
            && runContext.render(enableHttp2).as(Boolean.class).orElse(false);
        builder.withHttp2Disable(!http2);

        if (websocketPingInterval != null) {
            builder.withWebsocketPingInterval(runContext.render(websocketPingInterval).as(Duration.class).orElseThrow().toMillis());
        }

        return builder.build();
    }

    /**
     * Whether this connection needs the OkHttp backend. True only when it opts into HTTP/2 or a
     * websocket keepalive ping — features the default Vert.x backend can't provide. Keeps Vert.x
     * (the prior behavior) for every connection that doesn't ask for these.
     */
    public boolean useOkHttpBackend(RunContext runContext) throws IllegalVariableEvaluationException {
        boolean http2 = enableHttp2 != null
            && runContext.render(enableHttp2).as(Boolean.class).orElse(false);
        return http2 || websocketPingInterval != null;
    }

    private String normalizeBase64(RunContext runContext, Property<String> prop) throws IllegalVariableEvaluationException {
        return runContext.render(prop)
            .as(String.class)
            .map(s -> s.replaceAll("\\s", ""))
            .orElseThrow();
    }
}
