#!/usr/bin/env python3
"""
GlobalShop - démo + tests automatisés (Partie 2 et Partie 3 du sujet).

Usage (le cluster doit tourner : `docker compose up -d --build`) :
    pip install -r scripts/requirements.txt
    python scripts/demo.py                    # tout : fonctionnel + charge + chaos
    python scripts/demo.py --only functional  # inscription, produits, catalogue, commande
    python scripts/demo.py --only load        # 100 achats concurrents sur un même produit
    python scripts/demo.py --only chaos       # arrêt brutal d'un primaire pendant des écritures
    python scripts/demo.py --only chaos --shard asia --workers 30

Les résultats sont aussi écrits dans results/demo_results.json (nous les avons citer dans le rapport).
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

API = os.environ.get("API", "http://127.0.0.1:8000")
RUN = uuid.uuid4().hex[:6]
SHARDS = ["africa", "europe", "america", "asia"]
RESULTS: dict = {"run_id": RUN, "api": API}
CHECKS: list = []


# ----------------------------------------------------------------------------- utilitaires


def banner(title: str) -> None:
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def check(condition: bool, message: str) -> bool:
    CHECKS.append((condition, message))
    print(("  [OK]   " if condition else "  [ECHEC] ") + message)
    return condition


def get(path: str, **kw):
    r = requests.get(API + path, timeout=kw.pop("timeout", 15), **kw)
    r.raise_for_status()
    return r.json()


def post(path: str, payload: dict, timeout: float = 30):
    return requests.post(API + path, json=payload, timeout=timeout)


def percentile(values, p):
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def wait_cluster_ready(timeout: int = 240, allow_degraded: bool = False) -> None:
    print("Attente du cluster (réplication saine sur les 4 shards)...")
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        try:
            last = get("/cluster", timeout=5)
            ok = all(s["status"] == "OK" for s in last.values())
            if ok or (allow_degraded and all(s["primary_up"] for s in last.values())):
                print("  cluster prêt :", {k: v["status"] for k, v in last.items()})
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    print("Cluster pas prêt. Dernier état :", json.dumps(last, indent=2, default=str))
    print("Astuce : `docker compose down -v && docker compose up -d --build` pour repartir de zéro,")
    print("ou relancer avec --allow-degraded.")
    sys.exit(2)


def container_of(host: str) -> str:
    """mysql-europe-primary (service) -> globalshop-europe-primary (container_name)."""
    return "globalshop-" + host.split("mysql-", 1)[1]


# 1. fonctionnel


def step_functional() -> None:
    banner("PARTIE 2 - Fonctionnalités : inscription, produits, catalogue distribué, commande")
    clients = {}
    for region in SHARDS:
        r = post("/clients", {"name": f"Client {region}", "email": f"c-{RUN}-{region}@globalshop.test", "region": region})
        check(r.status_code == 200, f"inscription client {region} -> nœud {r.json().get('node')} (id {r.json().get('client_id')})")
        clients[region] = r.json()

    dup = post("/clients", {"name": "Doublon", "email": f"c-{RUN}-africa@globalshop.test", "region": "asia"})
    check(dup.status_code == 409, "email unique à l'échelle du cluster (doublon sur un autre shard refusé)")

    placement: dict = {}
    products = []
    for i in range(12):
        r = post("/products", {"name": f"Produit-{RUN}-{i:02d}", "description": "démo", "price": 10 + i, "stock": 50})
        body = r.json()
        products.append(body)
        placement[body["node"]] = placement.get(body["node"], 0) + 1
    print("  répartition des 12 produits par hash :", placement)
    ids = [p["product_id"] for p in products]
    check(len(set(ids)) == len(ids), "IDs produits globalement uniques (offset/increment par shard)")
    check(all(SHARDS[(p["product_id"] - 1) % 4] == p["node"] for p in products), "shard déductible de l'ID : (id-1) % 4")

    catalogue = get("/products?limit=500")
    present = {p["id"] for p in catalogue["products"]}
    check(set(ids) <= present, f"catalogue distribué : {catalogue['count']} produits lus sur les 4 shards")
    check(catalogue["partial"] is False, "lecture complète (aucun shard manquant)")

    order = post("/orders", {"client_id": clients["africa"]["client_id"], "product_id": products[0]["product_id"], "quantity": 2})
    check(order.status_code == 200, f"commande : {order.json()}")
    detail = get(f"/orders/{order.json()['order_id']}")
    check(detail["invoice_id"] is not None, f"commande #{detail['id']} + facture #{detail['invoice_id']} sur le nœud {detail['node']}")
    audit = get(f"/products/{products[0]['product_id']}/audit")
    check(audit["stock"] + audit["units_sold"] == 50, f"invariant stock : {audit['stock']} restant + {audit['units_sold']} vendu = 50")
    RESULTS["functional"] = {"placement": placement, "catalogue_count": catalogue["count"]}


# 2. charge


def step_load(n: int = 100, stock: int = 20) -> None:
    banner(f"PARTIE 3.1 - Test de charge : {n} achats simultanés du même produit (stock = {stock})")
    client = post("/clients", {"name": "Charge", "email": f"load-{RUN}@globalshop.test", "region": "europe"}).json()
    product = post("/products", {"name": f"Produit-Charge-{RUN}", "description": "charge", "price": 5, "stock": stock}).json()
    print(f"  produit #{product['product_id']} sur le nœud {product['node']}, client #{client['client_id']}")

    barrier = threading.Barrier(n)
    outcomes = []

    def buyer(_):
        barrier.wait()
        t = time.perf_counter()
        try:
            r = post("/orders", {"client_id": client["client_id"], "product_id": product["product_id"], "quantity": 1}, timeout=60)
            code = r.status_code
        except requests.RequestException:
            code = 0
        outcomes.append((code, (time.perf_counter() - t) * 1000))

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n) as ex:
        list(ex.map(buyer, range(n)))
    elapsed = time.perf_counter() - started

    ok = sum(1 for c, _ in outcomes if c == 200)
    refused = sum(1 for c, _ in outcomes if c == 409)
    other = n - ok - refused
    lat = [ms for _, ms in outcomes]
    audit = get(f"/products/{product['product_id']}/audit")
    print(f"  durée {elapsed:.2f}s -> {n / elapsed:.0f} req/s | latence p50 {percentile(lat, 50):.0f} ms, p95 {percentile(lat, 95):.0f} ms, max {max(lat):.0f} ms")
    print(f"  commandes acceptées : {ok} | refusées (stock épuisé) : {refused} | autres : {other}")
    check(ok == stock, f"exactement {stock} achats acceptés (pas de survente)")
    check(refused == n - stock, f"{n - stock} achats refusés proprement (HTTP 409)")
    check(other == 0, "aucune erreur technique")
    check(audit["stock"] == 0 and audit["orders_count"] == stock, f"stock final = {audit['stock']}, commandes en base = {audit['orders_count']}")
    RESULTS["load"] = {"requests": n, "stock": stock, "accepted": ok, "refused": refused, "other": other,
                       "duration_s": round(elapsed, 3), "throughput_rps": round(n / elapsed, 1),
                       "latency_ms": {"p50": round(percentile(lat, 50)), "p95": round(percentile(lat, 95)), "max": round(max(lat))},
                       "final_stock": audit["stock"], "orders_in_db": audit["orders_count"]}


# 3. chaos


def step_chaos(target: str = "europe", workers: int = 20, stock: int = 5000) -> None:
    banner(f"PARTIE 3.2 - Chaos : arrêt brutal du primaire du shard '{target}' pendant des écritures")
    if shutil.which("docker") is None:
        print("  docker CLI introuvable : lancez ce test depuis la machine hôte du cluster.")
        return
    wait_cluster_ready(timeout=120)
    before = get("/cluster")[target]
    old_primary = before["primary"]
    container = container_of(old_primary)

    name = next(f"Chaos-{RUN}-{i}" for i in range(500) if get(f"/route?name=Chaos-{RUN}-{i}")["node"] == target)
    product = post("/products", {"name": name, "description": "chaos", "price": 1, "stock": stock}).json()
    client_region = next(s for s in SHARDS if s != target)
    client = post("/clients", {"name": "Chaos", "email": f"chaos-{RUN}@globalshop.test", "region": client_region}).json()
    pid, cid = product["product_id"], client["client_id"]
    print(f"  produit #{pid} (stock {stock}) sur '{target}' ; primaire actuel = {old_primary} ({container})")

    events = []  # (t_relatif, http_code, order_id)
    stop = threading.Event()
    t0 = time.time()

    def worker():
        while not stop.is_set():
            code, oid = 0, None
            try:
                r = post("/orders", {"client_id": cid, "product_id": pid, "quantity": 1}, timeout=45)
                code = r.status_code
                if code == 200:
                    oid = r.json()["order_id"]
            except requests.RequestException:
                pass
            events.append((time.time() - t0, code, oid))
            time.sleep(0.02)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for t in threads:
        t.start()
    time.sleep(5)  # régime nominal

    t_kill = time.time() - t0
    print(f"  t={t_kill:.1f}s : docker kill {container}  (arrêt brutal, SIGKILL)")
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)

    t_failover = t_first_ok = t_first_fail = None
    deadline = time.time() + 90
    while time.time() < deadline:
        time.sleep(0.5)
        now_rel = time.time() - t0
        snap = list(events)
        after = [e for e in snap if e[0] > t_kill]
        if t_first_fail is None:
            fails = [e for e in after if e[1] != 200]
            t_first_fail = fails[0][0] if fails else None
        if t_failover is None and get("/cluster")[target]["failover_count"] > before["failover_count"]:
            t_failover = now_rel
            print(f"  t={now_rel:.1f}s : failover détecté (réplique promue)")
        if t_first_ok is None and t_first_fail is not None:
            oks = [e for e in after if e[1] == 200 and e[0] > t_first_fail]
            t_first_ok = oks[0][0] if oks else None
            if t_first_ok:
                print(f"  t={t_first_ok:.1f}s : premières écritures de nouveau acceptées")
        if t_first_ok is not None and now_rel > t_first_ok + 8:
            break
    stop.set()
    for t in threads:
        t.join(timeout=60)

    codes: dict = {}
    for _, code, _ in events:
        codes[code] = codes.get(code, 0) + 1
    acked = {oid for _, code, oid in events if code == 200}
    audit = get(f"/products/{pid}/audit")
    in_db = set(audit["order_ids"])
    lost = sorted(acked - in_db)
    unknown_committed = len(in_db - acked)
    outage = round(t_first_ok - t_first_fail, 1) if (t_first_ok and t_first_fail) else None
    after_state = get("/cluster")[target]

    print("\n  --- Résultats du test de chaos ---")
    print(f"  requêtes totales : {len(events)} | codes HTTP : {dict(sorted(codes.items()))}  (503 = refus CP, 0 = erreur réseau)")
    print(f"  commandes acquittées (HTTP 200) : {len(acked)} | présentes en base : {len(in_db)}")
    print(f"  fenêtre d'indisponibilité en écriture : {outage} s | nouveau primaire : {after_state['primary']}")
    check(after_state["failover_count"] > before["failover_count"], "failover automatique réalisé")
    check(t_first_ok is not None, "les écritures reprennent sans intervention humaine")
    check(audit["stock"] + audit["units_sold"] == stock, f"invariant stock : {audit['stock']} + {audit['units_sold']} = {stock}")
    if lost:
        print(f"  [PERTE] {len(lost)} commandes acquittées absentes après failover (réplication asynchrone, RPO > 0) : {lost[:10]}")
    else:
        print("  [OK]   aucune commande acquittée perdue")
    print(f"  commandes présentes en base mais non acquittées (résultat incertain au moment du kill) : {unknown_committed}")

    print(f"\n  Redémarrage de l'ancien primaire ({container})...")
    subprocess.run(["docker", "start", container], check=True, capture_output=True)
    rejoin = None
    deadline = time.time() + 120
    while time.time() < deadline:
        time.sleep(2)
        st = get("/cluster")[target]
        if st["status"] in ("OK", "NEEDS_RESYNC") and st["replica_up"]:
            rejoin = st
            break
    if rejoin is None:
        print("  l'ancien primaire n'a pas rejoint le cluster dans le délai imparti.")
    elif rejoin["status"] == "OK":
        print(f"  [OK]   {rejoin['replica']} a rejoint le cluster comme RÉPLIQUE de {rejoin['primary']} (récupération complète)")
    else:
        print(f"  [INFO] resynchronisation manuelle requise : {rejoin['last_error']}")
        print("         Cause attendue : l'ancien primaire détient des transactions validées mais non répliquées")
        print("         (réplication asynchrone). Voir README > 'Resynchroniser un nœud'.")

    RESULTS["chaos"] = {
        "target_shard": target, "killed": container, "workers": workers, "stock": stock,
        "http_codes": {str(k): v for k, v in sorted(codes.items())}, "acknowledged": len(acked), "in_database": len(in_db),
        "acknowledged_but_lost": len(lost), "committed_but_unacknowledged": unknown_committed,
        "write_outage_seconds": outage, "failover_detected_at_s": t_failover, "kill_at_s": round(t_kill, 1),
        "new_primary": after_state["primary"], "stock_invariant_ok": audit["stock"] + audit["units_sold"] == stock,
        "old_primary_final_status": rejoin["status"] if rejoin else "unknown",
        "events": get("/events"),
    }


#  main


def main() -> None:
    global API
    parser = argparse.ArgumentParser(description="Démo et tests GlobalShop")
    parser.add_argument("--api", default=API)
    parser.add_argument("--only", choices=["functional", "load", "chaos"])
    parser.add_argument("--shard", default="europe", choices=SHARDS, help="shard cible du test de chaos")
    parser.add_argument("--workers", type=int, default=20, help="écrivains concurrents pendant le chaos")
    parser.add_argument("--allow-degraded", action="store_true")
    args = parser.parse_args()
    API = args.api.rstrip("/")
    RESULTS["api"] = API

    wait_cluster_ready(allow_degraded=args.allow_degraded)
    if args.only in (None, "functional"):
        step_functional()
    if args.only in (None, "load"):
        step_load()
    if args.only in (None, "chaos"):
        step_chaos(args.shard, args.workers)

    os.makedirs("results", exist_ok=True)
    with open(os.path.join("results", "demo_results.json"), "w", encoding="utf-8") as fh:
        json.dump(RESULTS, fh, indent=2, ensure_ascii=False, default=str)
    failed = [m for ok, m in CHECKS if not ok]
    banner(f"BILAN : {len(CHECKS) - len(failed)}/{len(CHECKS)} vérifications réussies - résultats dans results/demo_results.json")
    for m in failed:
        print("  [ECHEC]", m)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
