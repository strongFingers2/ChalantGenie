package com.sarthak.chalantGenie.inventory;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {
    private final JdbcTemplate jdbcTemplate;
    public InventoryService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    @Transactional
    public boolean reserve(String orderId, String productId, int qty) {
        String command = "INSERT INTO processed_orders (order_id, product_id) VALUES (?, ?) ON CONFLICT (order_id, product_id) DO NOTHING";
        int dummy = jdbcTemplate.update(command, orderId, productId);
        String update = "UPDATE inventory set stock = stock - ? where product_id = ? and stock >= ?";
        int rows = jdbcTemplate.update(update, qty, productId, qty);
        String insertInOutbox = "INSERT INTO outbox (order_id, product_id, reserved) VALUES (?, ?, ?)";
        boolean reserved = false;
        if (rows > 0) reserved = true;
        int u = jdbcTemplate.update(insertInOutbox, orderId, productId, reserved);
        return reserved;
    }
}
