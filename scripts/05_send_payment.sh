#!/bin/bash
# Envía un pago de ejemplo al topic 'payments' via REST.
# La topología Kafka Streams lo procesará:
#   - Si amount > 1000 → se enruta también a 'payments-high-value'
#   - El contador global por status se actualiza en el state store (ver 06_get_payment_count.sh)
#   - El contador por ventana de 1 min se actualiza en el state store windowed (ver 07_get_windowed_count.sh)

curl -s -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -d '{
    "id": "PAY-001",
    "orderId": "ORD-001",
    "amount": 1500.00,
    "currency": "EUR",
    "status": "APPROVED"
  }' | jq .