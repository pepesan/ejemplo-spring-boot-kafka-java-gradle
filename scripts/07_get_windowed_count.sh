#!/bin/bash
# Consulta el state store windowed de Kafka Streams para ver cuántos pagos
# hay por estado en cada ventana de 1 minuto (última hora).
# Cambia el STATUS por PENDING, APPROVED o REJECTED.

STATUS="${1:-APPROVED}"

echo "Consultando contador windowed de pagos con status: $STATUS"
curl -s "http://localhost:8080/payments/count/${STATUS}/windowed" | jq .