package com.sarthak.chalantGenie.listener ;
// Keep your own package line above this. Imports assume the packages you've been using.
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import com.sarthak.chalantGenie.inventory.InventoryService;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class OrderPlacedListener {
    private static final Logger logger = LoggerFactory.getLogger(OrderPlacedListener.class);

    private final JsonMapper jsonMapper;
    private final InventoryService inventoryService;

    public OrderPlacedListener(JsonMapper jsonMapper, InventoryService inventoryService) {
        this.jsonMapper = jsonMapper;
        this.inventoryService = inventoryService;
    }

    public record Item(String product_id, int qty) {}
    public record OrderPlaced(String order_id, List<Item> products) {}

    @KafkaListener(topics = "order_placed", groupId = "inventory")
    void listener(String payload, Acknowledgment ack) {
        OrderPlaced orderPlaced;
        try {
            orderPlaced = jsonMapper.readValue(payload, OrderPlaced.class);
        } catch (JacksonException e) {
            // permanent failure: retrying the same bytes can never succeed
            logger.error("Could not parse payload, discarding: {}", payload, e);
            ack.acknowledge();
            return;
        }

        // validate the whole message before reserving anything
        String problem = validate(orderPlaced);
        if (problem != null) {
            logger.error("Invalid OrderPlaced, discarding: {}", problem);
            ack.acknowledge();
            return;
        }

        // no try/catch here on purpose: if reserve() throws, no ack,
        // Kafka redelivers, and (order_id, product_id) dedup skips finished items
        for (Item item : orderPlaced.products()) {
            inventoryService.reserve(orderPlaced.order_id(), item.product_id(), item.qty());
        }

        logger.info("Reserved all items for order {}", orderPlaced.order_id());
        // one ack per record, after every item's transaction has committed
        ack.acknowledge();
    }

    // returns a description of the first problem found, or null if the message is valid
    private String validate(OrderPlaced order) {
        if (order == null) {
            return "payload was JSON null";
        }
        if (order.order_id() == null || order.order_id().isBlank()) {
            return "missing or blank order_id";
        }
        if (order.products() == null || order.products().isEmpty()) {
            return "order " + order.order_id() + " has no products";
        }
        Set<String> seen = new HashSet<>();
        for (Item item : order.products()) {
            if (item == null || item.product_id() == null || item.product_id().isBlank()) {
                return "order " + order.order_id() + " has an item with a missing product_id";
            }
            if (item.qty() <= 0) {
                return "order " + order.order_id() + ", product " + item.product_id()
                        + " has non-positive qty " + item.qty();
            }
            if (!seen.add(item.product_id())) {
                return "order " + order.order_id() + " lists product " + item.product_id() + " twice";
            }
        }
        return null;
    }
}