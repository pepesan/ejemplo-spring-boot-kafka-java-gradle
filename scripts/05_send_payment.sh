#!/bin/bash
# Envía un pago de ejemplo al topic 'payments' via REST.
# La topología Kafka Streams lo procesará:
#   - Si amount > 1000 → se enruta también a 'payments-high-value'
#   - El contador por status se actualiza en el state store

curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "id": "PAY-001",
    "orderId": "ORD-001",
    "amount": 1500.00,
    "currency": "EUR",
    "status": "APPROVED"
  }' | jq .