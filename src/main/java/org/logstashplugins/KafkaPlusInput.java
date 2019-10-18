package org.logstashplugins;

import co.elastic.logstash.api.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@LogstashPlugin(name = "kafka_plus_input")
public class KafkaPlusInput implements Input {

    public static final PluginConfigSpec<String> BOOTSTRAP_SERVERS =
            PluginConfigSpec.stringSetting("bootstrap_servers", "localhost:9092");

    public static final PluginConfigSpec<String> GROUP_ID =
            PluginConfigSpec.stringSetting("groupId", "kafkaPlus");

    public static final PluginConfigSpec<List<Object>> TOPIC =
            PluginConfigSpec.arraySetting("topic", Arrays.asList("topic.logging"), false, false);

    private KafkaConsumer<String, String> consumer;

    private final CountDownLatch done = new CountDownLatch(1);
    private String id;
    private volatile boolean stopped;

    public KafkaPlusInput(String id, Configuration config, Context context) {
        this.id = id;
        String kafkaNodes = config.get(BOOTSTRAP_SERVERS);
        String aGroupId = config.get(GROUP_ID);
        List<Object>  topic = config.get(TOPIC);

        Map<String, Object> kConfig = new HashMap<>();
        kConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodes);
        kConfig.put(ConsumerConfig.GROUP_ID_CONFIG, aGroupId);


        consumer = new KafkaConsumer<>(kConfig);
        Collection data = new Vector(topic);
        consumer.subscribe(data);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        final int giveUp = 100;   int noRecordsCount = 0;

        while(true) {
            final ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(1000));
            if(consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                consumer.accept(Collections.singletonMap(record.key(), record.value()));
            });
        }
    }

    @Override
    public void stop() {
        this.consumer.unsubscribe();
        stopped = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
