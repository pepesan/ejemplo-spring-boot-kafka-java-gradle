package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderPaymentEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderPaymentEventRepository extends JpaRepository<OrderPaymentEvent, String> {
}