package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.OrderMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderMessageRepository extends JpaRepository<OrderMessage, String> {
}