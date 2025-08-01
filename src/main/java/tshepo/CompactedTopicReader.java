package tshepo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

public class CompactedTopicReader {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-reader");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            "user-profiles",
            new SimpleStringSchema(),
            props
        );
        consumer.setStartFromEarliest(); // Important for seeing full compacted state on restart

        env
            .addSource(consumer)
            .map(value -> "Received: " + value)
            .print();

        env.execute("Flink Reader from Redpanda Compacted Topic");
    }
}
