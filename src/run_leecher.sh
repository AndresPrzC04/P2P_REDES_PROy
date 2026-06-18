#!/bin/bash

if [ -z "$1" ]; then
    echo "Error: Debes especificar el nombre del archivo de salida."
    echo "Uso: ./run_leecher.sh <nombre_de_salida.ext>"
    exit 1
fi

ARCHIVO_SALIDA="$1"
TRACKER_IP="10.3.57.224" # <-- Cambia esto por la IP real de tu Tracker en el lab
PUERTO_LOCAL="6882"      # Puerto para escuchar a otros peers
NODE_ID=$(hostname)      # Usa el nombre de la PC del lab como ID único

echo "=== INICIANDO DESCARGA ENJAMBRE P2P ==="
echo "ID del Nodo: $NODE_ID"
echo "Guardando como: $ARCHIVO_SALIDA"
echo "Estrategia: Cooperativa (Máxima velocidad grupal)"
echo "------------------------------------------------"

python3 node.py \
    --tracker "$TRACKER_IP:9000" \
    --host "0.0.0.0" \
    --port "$PUERTO_LOCAL" \
    --out "$ARCHIVO_SALIDA" \
    --policy coop \
    --node-id "$NODE_ID"