package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class OrderPaymentEvent {
    private String orderId;
    private String customerId;
    private BigDecimal orderTotal;
    @Id
    private String paymentId;
    private BigDecimal paymentAmount;
    private String paymentCurrency;
    /** PENDING | APPROVED | REJECTED */
    private String paymentStatus;
}
