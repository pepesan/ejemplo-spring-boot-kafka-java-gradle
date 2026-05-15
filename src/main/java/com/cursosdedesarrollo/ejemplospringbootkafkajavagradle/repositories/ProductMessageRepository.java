package com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.repositories;

import com.cursosdedesarrollo.ejemplospringbootkafkajavagradle.models.ProductMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductMessageRepository extends JpaRepository<ProductMessage, String> {
}