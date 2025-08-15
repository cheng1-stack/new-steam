package com.steam_kafak;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class steam_ka {
    // 配置参数
    private static final String BOOTSTRAP_SERVERS = "cdh01:9092";
    private static final String GROUP_ID = "steam-consumer-group";
    private static final String TOPIC_NAME = "realtime_log";
    // 是否从最早位置开始消费（true: 从头消费，false: 从最新位置消费）
    private static final boolean CONSUME_FROM_BEGINNING = true;

    public static void main(String[] args) {
        // 1. 配置消费者属性
        Properties props = new Properties();

        // Kafka服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 消费者组ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // 反序列化器配置
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 关闭自动提交偏移量（手动控制）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 首次消费策略（仅当无偏移量记录时生效）
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 消费者拉取超时时间
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // 2. 创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            // 3. 订阅目标Topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            // 4. 如果需要从最早位置消费，手动重置偏移量
            if (CONSUME_FROM_BEGINNING) {
                // 确保订阅生效
                consumer.poll(Duration.ofMillis(100));
                // 获取当前订阅的分区
                Set<TopicPartition> partitions = consumer.assignment();
                // 重置所有分区到最早位置
                consumer.seekToBeginning(partitions);
                System.out.println("已重置偏移量到最早位置，开始消费历史数据...");
            }

            // 5. 循环消费数据
            while (true) {
                // 拉取数据（超时时间500ms）
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                // 处理消息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "消费数据 - Topic: %s, 分区: %d, 偏移量: %d, Key: %s, Value: %s%n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()
                    );
                }

                // 手动提交偏移量（确保消息处理完成后再提交）
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    System.out.println("已提交偏移量，当前消费位置已更新");
                }
            }
        } catch (Exception e) {
            System.err.println("消费过程发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭消费者，释放资源
            consumer.close();
            System.out.println("消费者已关闭");
        }
    }
}
