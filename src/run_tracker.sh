#!/bin/bash

# Validar si se pasó el nombre del archivo como argumento
if [ -z "$1" ]; then
    echo "Error: Debes proporcionar el archivo de la profesora."
    echo "Uso: ./run_tracker.sh <nombre_del_archivo>"
    exit 1
fi

ARCHIVO_PROFESORA="$1"
MANIFIESTO_SALIDA="manifest.json"

echo "=== CONFIGURANDO TRACKER CENTRAL ==="
echo "Archivo objetivo: $ARCHIVO_PROFESORA"

# 1. Crear el manifiesto a partir del archivo real dado
python3 manifest.py crear "$ARCHIVO_PROFESORA" "$MANIFIESTO_SALIDA" --piece-size 16777216

# 2. Arrancar el tracker
echo "Iniciando Tracker en el puerto 9000..."
python3 tracker.py "$MANIFIESTO_SALIDA" --host 0.0.0.0 --port 9000