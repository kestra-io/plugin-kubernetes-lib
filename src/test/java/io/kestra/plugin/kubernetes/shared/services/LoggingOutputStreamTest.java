package io.kestra.plugin.kubernetes.shared.services;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.tasks.runners.AbstractLogConsumer;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingOutputStreamTest {
    @Test
    void stripsTheKubernetesTimestampPrefix() throws Exception {
        var consumer = new CollectingLogConsumer();

        write(consumer, "2026-09-15T10:00:00.123456789Z hello world\n");

        assertThat(consumer.lines).containsExactly("hello world");
    }

    @Test
    void keepsTheMessageVerbatimAfterTheTimestamp() throws Exception {
        var consumer = new CollectingLogConsumer();

        // Splitting on every whitespace run and rejoining used to collapse these into single spaces.
        write(consumer, "2026-09-15T10:00:00.123456789Z column1    column2\n");

        assertThat(consumer.lines).containsExactly("column1    column2");
    }

    @Test
    void keepsAnOtlpFrameIntactSoTheLogMatcherCanParseIt() throws Exception {
        var consumer = new CollectingLogConsumer();
        var frame = "::{\"otlp\":{\"body\":{\"stringValue\":\"a  b\"}}}::";

        write(consumer, "2026-09-15T10:00:00.123456789Z " + frame + "\n");

        assertThat(consumer.lines).containsExactly(frame);
    }

    @Test
    void leavesALineWithoutATimestampAlone() throws Exception {
        var consumer = new CollectingLogConsumer();

        write(consumer, "not  a  timestamp\n");

        assertThat(consumer.lines).containsExactly("not  a  timestamp");
    }

    @Test
    void emitsAnEmptyMessageForATimestampOnlyLine() throws Exception {
        var consumer = new CollectingLogConsumer();

        write(consumer, "2026-09-15T10:00:00.123456789Z\n");

        assertThat(consumer.lines).containsExactly("");
    }

    @Test
    void tracksTheLatestTimestampSeen() throws Exception {
        var consumer = new CollectingLogConsumer();

        try (var stream = new LoggingOutputStream(consumer)) {
            stream.write("2026-09-15T10:00:02Z later\n".getBytes(StandardCharsets.UTF_8));
            stream.write("2026-09-15T10:00:01Z earlier\n".getBytes(StandardCharsets.UTF_8));
            stream.flush();

            assertThat(stream.getLastTimestamp()).isEqualTo(Instant.parse("2026-09-15T10:00:02Z"));
        }
    }

    private static void write(AbstractLogConsumer consumer, String payload) throws Exception {
        try (var stream = new LoggingOutputStream(consumer)) {
            stream.write(payload.getBytes(StandardCharsets.UTF_8));
            stream.flush();
        }
    }

    private static final class CollectingLogConsumer extends AbstractLogConsumer {
        private final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public void accept(String line, Boolean isStdErr) {
            lines.add(line);
        }

        @Override
        public void accept(String line, Boolean isStdErr, Instant instant) {
            this.accept(line, isStdErr);
        }
    }
}
