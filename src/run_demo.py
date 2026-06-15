"""
run_demo.py
-----------
Banco de pruebas reproducible. Levanta en localhost:
    - 1 tracker
    - 1 nodo origen (seed) con el archivo completo
    - N estudiantes (leechers) que descargan en paralelo

Mide el MAKESPAN (tiempo hasta que el ULTIMO estudiante termina) y verifica que
cada copia descargada sea identica al original (SHA-256). Sirve para la Fase 3:
correr con --policy coop y luego --policy tft y comparar.

Ejemplos:
    python3 run_demo.py --size-mb 64 --leechers 8 --policy coop
    python3 run_demo.py --size-mb 64 --leechers 8 --policy tft
"""

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time

import protocol as P
from manifest import build_manifest, save_manifest, load_manifest

HERE = os.path.dirname(os.path.abspath(__file__))


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def bits_complete(bits_path: str, num_pieces: int) -> bool:
    if not os.path.exists(bits_path):
        return False
    try:
        with open(bits_path, "rb") as f:
            return P.count_bits(bytearray(f.read())) >= num_pieces
    except Exception:  # noqa: BLE001
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size-mb", type=int, default=32, help="tamano del archivo de prueba")
    ap.add_argument("--leechers", type=int, default=6)
    ap.add_argument("--policy", choices=["coop", "tft"], default="coop")
    ap.add_argument("--piece-size", type=int, default=512 * 1024)
    ap.add_argument("--tracker-port", type=int, default=9000)
    ap.add_argument("--base-port", type=int, default=6881)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--seed-up-kbps", type=int, default=0,
                    help="limite de subida del ORIGEN en KB/s (emula servidor limitado)")
    ap.add_argument("--leech-up-kbps", type=int, default=0,
                    help="limite de subida de cada estudiante en KB/s")
    ap.add_argument("--keep", action="store_true", help="no borrar el directorio temporal")
    args = ap.parse_args()

    work = tempfile.mkdtemp(prefix="p2p_demo_")
    print(f"== Directorio de trabajo: {work}")
    logs = os.path.join(work, "logs"); os.makedirs(logs)

    # 1) archivo de prueba + manifiesto
    src = os.path.join(work, "original.bin")
    print(f"== Generando archivo de prueba de {args.size_mb} MB ...")
    with open(src, "wb") as f:
        f.write(os.urandom(args.size_mb * 1024 * 1024))
    orig_hash = sha256_file(src)
    manifest = build_manifest(src, args.piece_size)
    man_path = os.path.join(work, "manifest.json")
    save_manifest(manifest, man_path)
    num_pieces = manifest["num_pieces"]
    print(f"== Piezas: {num_pieces} de {args.piece_size//1024} KB c/u | "
          f"hash original {orig_hash[:12]}...")

    procs = []

    def launch(name, cmd):
        out = open(os.path.join(logs, name + ".log"), "w")
        p = subprocess.Popen([sys.executable] + cmd, cwd=HERE, stdout=out, stderr=out)
        procs.append((name, p, out))
        return p

    try:
        # 2) tracker
        launch("tracker", ["tracker.py", man_path,
                           "--host", "127.0.0.1", "--port", str(args.tracker_port)])
        time.sleep(1.0)

        tracker_addr = f"127.0.0.1:{args.tracker_port}"

        # 3) seed
        seed_cmd = ["node.py", "--tracker", tracker_addr, "--host", "127.0.0.1",
                    "--port", str(args.base_port), "--seed", "--data", src,
                    "--node-id", "SEED"]
        if args.seed_up_kbps:
            seed_cmd += ["--up-kbps", str(args.seed_up_kbps)]
        launch("seed", seed_cmd)
        time.sleep(0.5)

        # 4) leechers
        outs = []
        t0 = time.time()
        for i in range(args.leechers):
            port = args.base_port + 1 + i
            out_path = os.path.join(work, f"leecher_{i}.bin")
            outs.append(out_path)
            leech_cmd = ["node.py", "--tracker", tracker_addr,
                         "--host", "127.0.0.1", "--port", str(port),
                         "--out", out_path, "--policy", args.policy,
                         "--node-id", f"L{i}"]
            if args.leech_up_kbps:
                leech_cmd += ["--up-kbps", str(args.leech_up_kbps)]
            launch(f"leecher_{i}", leech_cmd)
        print(f"== {args.leechers} estudiantes lanzados (policy={args.policy}). "
              f"Esperando descargas ...")

        # 5) esperar a que todos completen
        finish = {}
        while len(finish) < args.leechers and time.time() - t0 < args.timeout:
            for i, out_path in enumerate(outs):
                if i in finish:
                    continue
                if bits_complete(out_path + ".bits", num_pieces):
                    finish[i] = time.time() - t0
                    print(f"   - estudiante {i} termino a los {finish[i]:0.2f}s")
            time.sleep(0.2)

        # 6) resultados
        print("\n===================  RESULTADOS  ===================")
        print(f"Politica          : {args.policy}")
        print(f"Estudiantes        : {args.leechers}")
        print(f"Tamano archivo     : {args.size_mb} MB ({num_pieces} piezas)")
        if len(finish) == args.leechers:
            makespan = max(finish.values())
            primero = min(finish.values())
            print(f"Primer nodo listo  : {primero:0.2f}s")
            print(f"MAKESPAN (ultimo)  : {makespan:0.2f}s   <-- metrica de la competencia")
            agg_mb = args.size_mb * args.leechers
            print(f"Throughput agregado: {agg_mb/makespan:0.1f} MB/s")
        else:
            print(f"INCOMPLETO: solo {len(finish)}/{args.leechers} terminaron "
                  f"(timeout {args.timeout}s)")

        # 7) verificacion de integridad de cada copia
        print("\nVerificacion de integridad (SHA-256 vs original):")
        ok = 0
        for i, out_path in enumerate(outs):
            if os.path.exists(out_path) and os.path.getsize(out_path) == manifest["file_size"]:
                match = sha256_file(out_path) == orig_hash
                ok += match
                print(f"   - estudiante {i}: {'OK' if match else 'CORRUPTO'}")
            else:
                print(f"   - estudiante {i}: incompleto")
        print(f"\nCopias integras: {ok}/{args.leechers}")
        print("====================================================")

    finally:
        for name, p, out in procs:
            p.terminate()
        time.sleep(0.5)
        for name, p, out in procs:
            try:
                p.kill()
            except Exception:  # noqa: BLE001
                pass
            out.close()
        if args.keep:
            print(f"\nLogs y archivos en: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
