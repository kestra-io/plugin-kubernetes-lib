package io.kestra.plugin.kubernetes.shared.services;

import java.lang.reflect.Field;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.http.HttpClient;
import io.fabric8.kubernetes.client.impl.BaseClient;
import io.fabric8.kubernetes.client.vertx.VertxHttpClient;
import io.vertx.core.Vertx;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the native-memory fix in {@link ClientService}: every Vert.x-backed client must reuse a
 * single shared {@link Vertx} instead of building (and leaking) its own per task run. These tests
 * fail if someone reverts to the no-arg {@code VertxHttpClientFactory}.
 */
class ClientServiceTest {

    @Test
    void vertxBackedClientsReuseTheSharedVertx() throws Exception {
        var config = new ConfigBuilder().build();

        try (
            KubernetesClient c1 = ClientService.of(config, false);
            KubernetesClient c2 = ClientService.of(config, false)
        ) {

            // The point of the fix: no per-client Vertx — they all share one.
            assertThat(vertxOf(c1)).isSameAs(vertxOf(c2));

            // Shared, so a per-run client must not own it (closing a client won't tear it down).
            assertThat(closeVertxFlagOf(c1)).isFalse();
            assertThat(closeVertxFlagOf(c2)).isFalse();
        }
    }

    @Test
    void sharedVertxSurvivesClientClose() throws Exception {
        var config = new ConfigBuilder().build();

        Vertx shared;
        try (KubernetesClient first = ClientService.of(config, false)) {
            shared = vertxOf(first);
        }

        // A client built after earlier ones were closed still gets the same live Vertx.
        try (KubernetesClient later = ClientService.of(config, false)) {
            assertThat(vertxOf(later)).isSameAs(shared);
        }
    }

    private static Vertx vertxOf(KubernetesClient client) throws Exception {
        return (Vertx) readField(((BaseClient) client).getHttpClient(), "vertx");
    }

    private static boolean closeVertxFlagOf(KubernetesClient client) throws Exception {
        return (boolean) readField(((BaseClient) client).getHttpClient(), "closeVertx");
    }

    private static Object readField(HttpClient httpClient, String name) throws Exception {
        // Fail with a clear message if the default backend is no longer Vert.x, rather than a
        // confusing reflection error further down.
        assertThat(httpClient).isInstanceOf(VertxHttpClient.class);

        Field field = httpClient.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(httpClient);
    }
}
