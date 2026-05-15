package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models;

import jakarta.persistence.CollectionTable;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "order_messages")
public class OrderMessage {
    @Id
    @NotBlank
    private String id;
    @NotBlank
    private String customerId;
    @NotEmpty @Valid
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "order_lines", joinColumns = @JoinColumn(name = "order_id"))
    private List<OrderLine> lines;
    @NotNull @DecimalMin("0.01")
    private BigDecimal total;
    @NotNull
    private Instant createdAt;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @Embeddable
    public static class OrderLine {
        @NotBlank
        private String productId;
        @NotBlank
        private String productName;
        @NotNull @Min(1)
        private Integer quantity;
        @NotNull @DecimalMin("0.01")
        private BigDecimal unitPrice;
    }
}