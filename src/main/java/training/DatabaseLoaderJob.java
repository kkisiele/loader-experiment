package training;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serializable;

import static org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;

public class DatabaseLoaderJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);
        env.disableOperatorChaining();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("txlog_1m")
                .setGroupId("txlog_1m")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSink<Message> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(new Transform())
                .name(Transform.class.getSimpleName())
                .addSink(batch(DatabaseLoaderJob::toSql)).name("postgresql sink");

        env.execute();
    }

    static <T> SinkFunction<T> batch(BatchPlainStatementsExecutor.PlainSqlFactory<T> sqlFactory) {
        return new GenericJdbcSinkFunction<>(
                new JdbcOutputFormat<>(
                        new SimpleJdbcConnectionProvider(jdbcConnectionOptions()),
                        defaultJdbcOptions(),
                        context -> new BatchPlainStatementsExecutor<>(sqlFactory),
                        JdbcOutputFormat.RecordExtractor.identity()));
    }

    private static String toSql(Message message) {
        switch (message.operation) {
            case INSERT: return String.format("INSERT INTO txlog(id,text) values (%d, '%s') ON CONFLICT ON CONSTRAINT txlog_pkey DO NOTHING", message.id, message.text);
            case UPDATE: return String.format("UPDATE txlog set text='%s' where id=%d", message.text, message.id);
            case DELETE: return String.format("DELETE from txlog where id=%d", message.id);
        }
        return null;
    }

    private static JdbcExecutionOptions defaultJdbcOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(2000)
                .withMaxRetries(5)
                .build();
    }

    private static JdbcConnectionOptions jdbcConnectionOptions() {
        return new JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://localhost:5555/postgres")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("postgres")
                .build();
    }

    static class Message implements Serializable {
        public Operation operation;
        public Long id;
        public String text;

        public Message(Operation operation, Long id, String text) {
            this.operation = operation;
            this.id = id;
            this.text = text;
        }

        @Override
        public String toString() {
            return "Message{" +
                    "operation=" + operation +
                    ", id=" + id +
                    ", text='" + text + '\'' +
                    '}';
        }
    }

    enum Operation {
        INSERT, UPDATE, DELETE;
    }

    private static class Transform extends RichMapFunction<String, Message> {
        private transient Counter total;
        private transient Counter insertTotal;
        private transient Counter updateTotal;
        private transient Counter deleteTotal;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            total = getRuntimeContext().getMetricGroup().counter("loader_all_records_total");
            insertTotal = getRuntimeContext().getMetricGroup().counter("loader_insert_records_total");
            updateTotal = getRuntimeContext().getMetricGroup().counter("loader_update_records_total");
            deleteTotal = getRuntimeContext().getMetricGroup().counter("loader_delete_records_total");
        }

        @Override
        public Message map(String value) {
            total.inc();
            String[] items = value.split(";");
            switch (items[0]) {
                case "I":
                    insertTotal.inc();
                    return new Message(Operation.INSERT, Long.valueOf(items[1]), items[2]);
                case "U":
                    updateTotal.inc();
                    return new Message(Operation.UPDATE, Long.valueOf(items[1]), items[2]);
                case "D":
                    deleteTotal.inc();
                    return new Message(Operation.DELETE, Long.valueOf(items[1]), items[2]);
            }
            throw new RuntimeException();
        }
    }
}
