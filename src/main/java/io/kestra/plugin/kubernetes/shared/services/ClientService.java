package io.kestra.plugin.kubernetes.shared.services;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory;
import io.fabric8.kubernetes.client.vertx.VertxHttpClientFactory;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;

abstract public class ClientService {
    /**
     * One {@link Vertx} shared by every Vert.x-backed {@link KubernetesClient}, created once and
     * reused. A fresh one per task run would leak threads and off-heap memory until the pod is
     * OOM-killed; sharing avoids that (closing a client only drops its own connections).
     */
    private static final class SharedVertxHolder {
        private static final String DISABLE_DNS_RESOLVER = "vertx.disableDnsResolver";
        private static final Vertx INSTANCE = createSharedVertx();

        private static Vertx createSharedVertx() {
            // Same options fabric8 uses internally: daemon threads (this is never closed), no file
            // cache, and the async DNS resolver off (it breaks in-cluster name resolution).
            String original = System.getProperty(DISABLE_DNS_RESOLVER);
            try {
                System.setProperty(DISABLE_DNS_RESOLVER, "true");
                return Vertx.vertx(
                    new VertxOptions()
                        .setFileSystemOptions(
                            new FileSystemOptions()
                                .setFileCachingEnabled(false)
                                .setClassPathResolvingEnabled(false)
                        )
                        .setUseDaemonThread(true)
                );
            } finally {
                if (original == null) {
                    System.clearProperty(DISABLE_DNS_RESOLVER);
                } else {
                    System.setProperty(DISABLE_DNS_RESOLVER, original);
                }
            }
        }
    }

    /**
     * Creates an {@link KubernetesClientBuilder} which is pre-configured from the cluster configuration.
     * loading the in-cluster config, including:
     * 1. System properties
     * 2. Environment variables
     * 3. Kube config file
     * 4. Service account token and a mounted CA certificate
     *
     * @return {@link KubernetesClient} configured from the cluster configuration
     */
    public static KubernetesClient of() {
        return of(Config.autoConfigure(null), false);
    }

    /**
     * Creates a {@link KubernetesClient} from a {@link Config}, keeping the default Vert.x backend.
     *
     * @param config The {@link Config} to configure the builder from.
     * @return {@link DefaultKubernetesClient} configured from the provided {@link Config}
     */
    public static KubernetesClient of(Config config) {
        return of(config, false);
    }

    /**
     * Creates a {@link KubernetesClient} from a {@link Config}, choosing the HTTP backend explicitly.
     *
     * @param useOkHttpBackend when true, use OkHttp (supports HTTP/2 and websocket keepalive pings);
     *        when false, keep the default Vert.x backend so existing behavior is unchanged.
     *        The backend is pinned explicitly because OkHttp outranks Vert.x by
     *        service-loader priority, so classpath order alone can't be trusted.
     */
    public static KubernetesClient of(Config config, boolean useOkHttpBackend) {
        return new KubernetesClientBuilder()
            .withConfig(config)
            .withHttpClientFactory(
                useOkHttpBackend
                    ? new OkHttpClientFactory()
                    : new VertxHttpClientFactory(SharedVertxHolder.INSTANCE)
            )
            .build();
    }
}
