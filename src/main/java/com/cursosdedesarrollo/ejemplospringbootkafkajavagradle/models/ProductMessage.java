package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
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
public class ProductMessage {
    @Id
    @NotBlank
    private String id;
    @NotBlank
    private String name;
    private String description;
    @NotNull @DecimalMin("0.01")
    private BigDecimal price;
    @NotNull @Min(0)
    private Integer stock;
}