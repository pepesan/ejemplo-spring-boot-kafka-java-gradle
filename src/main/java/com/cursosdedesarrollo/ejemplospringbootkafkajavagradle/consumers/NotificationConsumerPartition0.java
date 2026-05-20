package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.consumers;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.NotificationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class NotificationConsumerPartition0 {

    private final List<NotificationMessage> received = new ArrayList<>();

    @KafkaListener(
        groupId = "${spring.application.name}-notifications-p0",
        topicPartitions = @TopicPartition(
            topic = "notifications",
            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")
        )
    )
    public void consume(NotificationMessage notification) {
        log.info("[CONSUMER][P0] notifications partición 0 | id={} | recipient={} | subject={}",
                notification.getId(), notification.getRecipient(), notification.getSubject());
        received.add(notification);
    }

    public List<NotificationMessage> getReceived() {
        return Collections.unmodifiableList(received);
    }

    public void clearAll() {
        received.clear();
    }
}
