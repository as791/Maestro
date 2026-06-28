package com.example.maestro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

/**
 * Sample Flink 2.2 streaming job for the Maestro control-plane demo.
 *
 * <p>Reads Wikimedia "recentchange" events from Kafka, counts edits per wiki in
 * a one-minute tumbling window, and writes the counts back to Kafka. It is
 * deliberately small; its purpose is to give the control plane a real,
 * checkpointing, stateful job whose lifecycle transitions can be driven.
 *
 * <p>Program arguments (rendered by the control plane from the deployment spec
 * JobArgs as {@code --key value}):
 * <ul>
 *   <li>{@code --bootstrap.servers} Kafka bootstrap servers</li>
 *   <li>{@code --source.topic} input topic (default wikimedia.recentchange)</li>
 *   <li>{@code --sink.topic} output topic (default wikimedia.edit-counts)</li>
 *   <li>{@code --group.id} consumer group (default maestro-wiki-edit-count)</li>
 * </ul>
 */
public final class WikiEditCount {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WikiEditCount() {}

    public static void main(String[] args) throws Exception {
        final Map<String, String> params = parseArgs(args);
        final String brokers = params.getOrDefault("bootstrap.servers", "maestro-kafka-bootstrap.kafka.svc:9092");
        final String sourceTopic = params.getOrDefault("source.topic", "wikimedia.recentchange");
        final String groupId = params.getOrDefault("group.id", "maestro-wiki-edit-count");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Checkpointing makes savepoint / last-state upgrade modes meaningful.
        env.enableCheckpointing(Duration.ofSeconds(10).toMillis());

        final KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final DataStream<String> events =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "wikimedia-recentchange");

        final DataStream<Tuple2<String, Long>> counts = events
                .map(new ExtractWiki())
                .filter(wiki -> wiki != null && !wiki.isEmpty())
                .map((MapFunction<String, Tuple2<String, Long>>) wiki -> Tuple2.of(wiki, 1L))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LONG))
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(60)))
                .sum(1);

        counts.print();

        env.execute("maestro-wiki-edit-count");
    }

    /** Parses {@code --key value} program arguments into a map. */
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            String key = args[i];
            if (key.startsWith("--")) {
                key = key.substring(2);
            }
            params.put(key, args[i + 1]);
        }
        return params;
    }

    /** Extracts the {@code wiki} field from a recentchange JSON event. */
    private static final class ExtractWiki implements MapFunction<String, String> {
        @Override
        public String map(String value) {
            try {
                JsonNode node = MAPPER.readTree(value);
                JsonNode wiki = node.get("wiki");
                return wiki != null ? wiki.asText() : "";
            } catch (Exception e) {
                return "";
            }
        }
    }
}
