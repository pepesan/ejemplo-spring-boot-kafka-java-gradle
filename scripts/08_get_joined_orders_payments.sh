#!/bin/bash
# Consulta los eventos de join orders-payments consumidos en memoria.
# Requiere haber enviado primero un pedido (03_send_order.sh) y luego un pago
# con el mismo orderId (05_send_payment.sh usa orderId: ORD-001).

echo "Consultando eventos join orders-payments:"
curl -s http://localhost:8080/orders-payments/consumed | jq .