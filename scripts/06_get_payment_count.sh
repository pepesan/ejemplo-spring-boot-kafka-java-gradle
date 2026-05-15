#!/bin/bash
# Consulta el state store de Kafka Streams para ver cuántos pagos
# hay por estado. Cambia el STATUS por PENDING, APPROVED o REJECTED.

STATUS=${1:-APPROVED}

echo "Consultando contador de pagos con status: $STATUS"
curl -s http://localhost:8080/payments/count/$STATUS | jq .