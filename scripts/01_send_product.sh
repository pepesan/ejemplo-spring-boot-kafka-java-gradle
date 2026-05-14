#!/bin/bash

curl -X POST http://localhost:8080/products \
  -H "Content-Type: application/json" \
  -d '{
    "id": "1",
    "name": "TV 4K",
    "description": "Televisor 55 pulgadas",
    "price": 499.99,
    "stock": 10
  }'