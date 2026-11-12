package com.sarthak.chalantGenie.publisher;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper mapper;
    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, JsonMapper mapper) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.mapper = mapper;
    }

    record OutboxRow(long id, String order_id, String product_id, boolean reserved) {}
    record ItemEvent(String eventType, String order_id, String product_id) {}

    @Scheduled(fixedDelay = 1000)
    public void publish_pending() {
        List<OutboxRow> batch = jdbc.query("SELECT sequence_id, order_id, product_id, reserved FROM outbox " +
                        "WHERE sent_marker IS NULL ORDER BY sequence_id LIMIT 50",
                (rs, i) -> new OutboxRow(rs.getLong("sequence_id"), rs.getString("order_id"),
                        rs.getString("product_id"), rs.getBoolean("reserved")));

        for (OutboxRow row : batch) {
            try {
                String eventType = row.reserved() ? "ItemReserved":"ItemNotReserved";
                String topic = row.reserved() ? "item_reserved":"item_not_reserved";
                String json = mapper.writeValueAsString(
                        new ItemEvent(eventType, row.order_id(), row.product_id()));
                kafka.send(topic, row.order_id(), json).get(10, TimeUnit.SECONDS);
                jdbc.update("UPDATE outbox SET sent_marker = now() WHERE sequence_id = ?", row.id());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Failed to publish outbox row {}", row.id(), e);
                break; // leave it unsent, retry next poll, keep ordering
            }
        }
    }
}
