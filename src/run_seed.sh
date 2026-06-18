#!/bin/bash

if [ -z "$1" ]; then
    echo "Error: Debes proporcionar el archivo original para sembrar."
    echo "Uso: ./run_seed.sh <nombre_del_archivo>"
    exit 1
fi

ARCHIVO_SEED="$1"
TRACKER_IP="10.3.57.224" # <-- Cambia esto por la IP real de tu Tracker en el lab

echo "=== INICIANDO NODO SEED (ORIGEN) ==="
echo "Sembrando archivo: $ARCHIVO_SEED"
echo "Conectando al Tracker en: $TRACKER_IP:9000"
echo "----------------------------------------"

python3 node.py \
    --tracker "$TRACKER_IP:9000" \
    --host "0.0.0.0" \
    --port 6881 \
    --seed \
    --data "$ARCHIVO_SEED" \
    --node-id "SEED_PRINCIPAL"