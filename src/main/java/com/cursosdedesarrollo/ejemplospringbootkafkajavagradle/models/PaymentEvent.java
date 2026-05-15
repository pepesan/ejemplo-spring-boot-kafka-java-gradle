package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private String id;
    private String orderId;
    private BigDecimal amount;
    private String currency;
    /** PENDING | APPROVED | REJECTED */
    private String status;
}