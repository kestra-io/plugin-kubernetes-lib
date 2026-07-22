package io.kestra.plugin.kubernetes.shared.services;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.SecurityContextBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Covers the map-merging helpers shared by PodCreate (OSS) and the Kubernetes task runner (EE).
 */
class InstanceServiceUtilsTest {

    @Test
    void deepMergeOverrideWinsOnScalars() {
        Map<String, Object> base = Map.of("a", 1, "b", 2);
        Map<String, Object> override = Map.of("b", 3);

        Map<String, Object> result = InstanceService.deepMerge(base, override);

        assertThat(result.get("a"), is(1));
        assertThat(result.get("b"), is(3));
    }

    @Test
    @SuppressWarnings("unchecked")
    void deepMergeRecursesIntoNestedMaps() {
        Map<String, Object> base = Map.of("nested", Map.of("keep", "x", "replace", "old"));
        Map<String, Object> override = Map.of("nested", Map.of("replace", "new"));

        Map<String, Object> result = InstanceService.deepMerge(base, override);

        Map<String, Object> nested = (Map<String, Object>) result.get("nested");
        assertThat(nested.get("keep"), is("x"));
        assertThat(nested.get("replace"), is("new"));
    }

    @Test
    void deepMergeIgnoresNullOverrideValues() {
        Map<String, Object> base = Map.of("a", 1);
        Map<String, Object> override = new HashMap<>();
        override.put("a", null);

        Map<String, Object> result = InstanceService.deepMerge(base, override);

        assertThat(result.get("a"), is(1));
    }

    @Test
    void containerToMapConvertsKubernetesObject() {
        var securityContext = new SecurityContextBuilder()
            .withRunAsUser(1000L)
            .withReadOnlyRootFilesystem(true)
            .build();

        Map<String, Object> result = InstanceService.containerToMap(securityContext);

        assertThat(result.get("runAsUser"), is(1000));
        assertThat(result.get("readOnlyRootFilesystem"), is(true));
    }

    @Test
    void containerToMapReturnsEmptyMapForNull() {
        Map<String, Object> result = InstanceService.containerToMap(null);

        assertThat(result.isEmpty(), is(true));
        assertThat(result.get("anything"), is(nullValue()));
    }
}
