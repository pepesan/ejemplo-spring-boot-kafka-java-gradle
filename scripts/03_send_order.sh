#!/bin/bash

curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ORD-001",
    "customerId": "CUST-42",
    "lines": [
      {
        "productId": "1",
        "productName": "TV 4K",
        "quantity": 2,
        "unitPrice": 499.99
      },
      {
        "productId": "2",
        "productName": "Mando Universal",
        "quantity": 1,
        "unitPrice": 19.99
      }
    ],
    "total": 1019.97,
    "createdAt": "2026-05-14T19:00:00"
  }'