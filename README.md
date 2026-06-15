# Enjambre Híbrido Cooperativo P2P — Código y Laboratorio (Fase 3)

Implementación desde cero, en **Python (solo librería estándar)**, de un sistema de
distribución de archivos P2P estilo BitTorrent para el reto **RCP vs P2P**.

Implementa:
- **Random‑First** + **Rarest‑First** (selección de piezas)
- **Desbloqueo cooperativo** (`coop`) y **Tit‑for‑Tat** (`tft`) — configurable para comparar
- **Verificación SHA‑256** por pieza + hash global + reanudación en disco
- **Tracker ligero** que NO transfiere datos (plano de datos 100 % P2P)

---

## 1. Requisitos

- **Python 3.8 o superior.** Nada más. Sin `pip install`.
- Verifícalo: `python3 --version`

## 2. Archivos

| Archivo | Qué es |
|---|---|
| `protocol.py` | Protocolo entre pares (tramas binarias) + utilidades de bitfield |
| `manifest.py` | Genera/lee el manifiesto (`.json`): fragmentación lógica + hashes SHA‑256 |
| `tracker.py` | Coordinador central ligero (lista de pares + manifiesto) |
| `node.py` | **El nodo**: cliente + servidor. Aquí viven los tres algoritmos |
| `run_demo.py` | Banco de pruebas: levanta todo en localhost, mide *makespan* y verifica integridad |

## 3. Arranque rápido (todo en una máquina)

```bash
# 8 estudiantes descargando un archivo de 64 MB con la política cooperativa
python3 run_demo.py --size-mb 64 --leechers 8 --policy coop
```

Genera un archivo aleatorio, levanta tracker + origen + 8 estudiantes, mide el *makespan*
y comprueba que las 8 copias sean idénticas al original (SHA‑256). Salida típica:

```
MAKESPAN (ultimo)  : 6.87s   <-- metrica de la competencia
Copias integras: 8/8
```

> **Nota honesta sobre localhost:** en *loopback* el ancho de banda es prácticamente
> ilimitado y perfectamente justo, así que (a) los tiempos **no** representan los de una
> LAN real y (b) `coop` y `tft` salen casi iguales. La diferencia entre políticas y la
> ventaja real del P2P aparecen **bajo restricción de ancho de banda** (sección 6) o, mejor
> aún, **en hardware real** (sección 5).

## 4. Comparar `coop` vs `tft` (para el informe)

```bash
python3 run_demo.py --size-mb 64 --leechers 12 --policy coop
python3 run_demo.py --size-mb 64 --leechers 12 --policy tft
```

Anota el *makespan* de cada uno en la tabla de resultados (sección 8).

## 5. Despliegue REAL en el salón (la prueba que cuenta)

Esto es lo que demuestra de verdad la ventaja del P2P. Necesitas las laptops en la **misma
red** (idealmente **por cable**; ver nota de topología al final).

### Paso 1 — En el servidor/origen: generar el manifiesto del archivo real

```bash
# piezas de 2 MiB (recomendado para 4 GB)
python3 manifest.py crear  pelicula_4gb.bin  manifest.json  --piece-size 2097152
```

Copia `manifest.json` (es pequeño) a todas las laptops, junto con los `.py`.

### Paso 2 — Levantar el tracker (en una máquina accesible por todas)

```bash
python3 tracker.py manifest.json --host 0.0.0.0 --port 9000
# anota la IP del tracker, p. ej. 192.168.1.10
```

### Paso 3 — Levantar el nodo origen (el que tiene el archivo completo)

```bash
python3 node.py --tracker 192.168.1.10:9000 --port 6881 --seed --data pelicula_4gb.bin
```

### Paso 4 — En cada laptop de estudiante

```bash
python3 node.py --tracker 192.168.1.10:9000 --port 6881 --out pelicula_4gb.bin --policy coop
```

Cada estudiante mostrará `COMPLETO en X.XXs` al terminar. El **makespan** es el mayor de
esos tiempos. Al finalizar, cada uno puede verificar su copia:

```bash
# Linux/Mac
sha256sum pelicula_4gb.bin
# comparar contra el "file_hash" que imprime manifest.py / está en manifest.json
```

> **Tip de medición justa:** lanza a todos los estudiantes lo más simultáneamente posible
> (cuenta regresiva, o un script `ssh`). El cronómetro arranca cuando arranca el primero.

## 6. Emular el salón en UNA máquina (recursos limitados + intermitencias)

### 6.1 Limitar el ancho de banda (integrado)

Para que la diferencia entre políticas y la ventaja del P2P se note sin 50 laptops,
estrangula la subida. Esto **emula el "servidor de recursos limitados"** del reto:

```bash
# Origen limitado a 2 MB/s. Un servidor unico tardaria N*F/2MBs;
# el enjambre reparte y termina mucho antes.
python3 run_demo.py --size-mb 64 --leechers 12 --policy coop --seed-up-kbps 2048

# Tambien puedes limitar a los estudiantes para simular laptops modestas:
python3 run_demo.py --size-mb 64 --leechers 12 --policy coop \
        --seed-up-kbps 2048 --leech-up-kbps 4096
```

El **throughput agregado** que reporta debe **superar** el límite del origen: esa es la
prueba numérica de que los pares están aportando capacidad. *(En nuestras pruebas, con el
origen a 2 MB/s y 8 nodos, el enjambre alcanzó 7.4 MB/s ≈ 3.7× la capacidad del origen.)*

### 6.2 Emular intermitencias y latencia de red (Linux, `tc`)

Para reproducir "intermitencias en la red", usa el control de tráfico del kernel sobre la
interfaz de loopback:

```bash
# añade 20 ms de latencia y 2% de pérdida de paquetes
sudo tc qdisc add dev lo root netem delay 20ms loss 2%

# ... corre tu prueba ...

# quitar la emulación al terminar
sudo tc qdisc del dev lo root netem
```

También puedes **matar y reiniciar** un proceso de estudiante a mitad de descarga: gracias
a la persistencia (`.bits`) y a los timeouts, **reanuda** y el enjambre se recupera. Es una
demo de robustez muy vistosa para el jurado.

## 7. Configuración avanzada

Parámetros sintonizables al inicio de `node.py` (constantes en MAYÚSCULAS): tamaño de
slots de subida, peticiones en vuelo por par, intervalos de desbloqueo y anuncio, timeout
y umbral de *endgame*. Valores por defecto razonables ya están puestos.

```bash
python3 node.py --help
python3 run_demo.py --help
```

---

## 8. Plantilla de registro de resultados (rellenar en la Fase 3)

### Tabla A — Escalabilidad P2P (política `coop`)

| Estudiantes | Tamaño | Makespan (s) | Throughput agregado (MB/s) | Integridad |
|---|---|---|---|---|
| 4  | 64 MB |  |  |  |
| 8  | 64 MB |  |  |  |
| 16 | 64 MB |  |  |  |
| 32 | 64 MB |  |  |  |

### Tabla B — `coop` vs `tft` (con origen limitado, p. ej. 2 MB/s)

| Política | Estudiantes | Makespan (s) | 1er nodo (s) | Δ (último − 1ro) |
|---|---|---|---|---|
| coop |  |  |  |  |
| tft  |  |  |  |  |

### Tabla C — **P2P vs Cliente‑Servidor (RCP)** — la comparación que decide

| Arquitectura | Estudiantes | Makespan (s) | Notas |
|---|---|---|---|
| P2P (enjambre) |  |  |  |
| Cliente‑servidor |  |  | servidor único = `N · F / uplink` |

### Tabla D — Robustez

| Escenario | Resultado |
|---|---|
| Se reinicia un estudiante a mitad de descarga | ¿reanuda? ¿integridad final? |
| Se cae el origen tras sembrar el enjambre | ¿completan los demás? |
| 2% de pérdida de paquetes (`tc netem`) | makespan vs sin pérdida |

---

## 9. Esqueleto del informe final (Fase 3)

1. **Configuración del entorno** — versiones, red usada (cable/WiFi), cómo se lanzó.
2. **Pruebas realizadas** — tablas A–D con los números obtenidos.
3. **Análisis** —
   - ¿Cuánto más rápido fue P2P que cliente‑servidor? ¿Coincide con `~N×`?
   - ¿`coop` igualó o superó a `tft`? Explica por qué en un enjambre cooperativo el
     desbloqueo cooperativo no se queda atrás y protege el *makespan*.
   - ¿Cómo se comportó la robustez ante reinicios/pérdidas?
4. **Limitaciones honestas** — efecto de la topología (WiFi compartido vs cable), tamaño
   de archivo de prueba vs los 4 GB reales.
5. **Conclusión** — qué arquitectura fue más eficiente y por qué.

---

## Nota de topología (importante para interpretar los números)

El modelo "P2P ≈ `N×` más rápido" asume **enlaces independientes** (switch cableado,
full‑duplex). En **WiFi compartido**, todo el tráfico cruza el mismo aire del access point
y el paralelismo se topa con ese techo: P2P **sigue ganando** (evita el cuello del uplink
único del servidor), pero el margen es menor. **Si pueden, prueben por cable.** Documentar
esta diferencia da puntos de rigor.

## Limitaciones conocidas / posibles mejoras

- Las peticiones son a **pieza completa** (sin sub‑bloques). Añadir *pipelining* por
  bloques de 16 KB mejoraría el solapamiento en enlaces de alta latencia.
- La **súper‑siembra** está descrita y el reparto por rareza la aproxima; una versión
  estricta (el origen evita servir piezas que ya están en el enjambre) la optimizaría aún
  más.
- Escrituras a disco síncronas: para 4 GB en hardware lento, podría moverse a un executor
  de hilos.
