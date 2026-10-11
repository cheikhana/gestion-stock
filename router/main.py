"""
GlobalShop - routeur / coordinateur de la base de données distribuée.

Rôles du routeur
----------------
1. ROUTAGE      : oriente chaque requête vers le bon shard (africa, europe, america, asia).
2. COHÉRENCE    : un seul écrivain par donnée (le primaire de son shard) + verrou de ligne
                  (SELECT ... FOR UPDATE) => pas de survente, pas de conflit multi-maître.
3. RÉSILIENCE   : supervise les 8 nœuds MySQL (4 primaires + 4 répliques), configure la
                  réplication GTID, promeut automatiquement la réplique si le primaire tombe
                  (failover) et rattache l'ancien primaire comme réplique à son retour.

Partitionnement
---------------
- Clients  : par RÉGION géographique.
- Produits : par HASH (crc32 du nom normalisé) modulo 4.
- Commandes + factures : sur le shard du PRODUIT (colocalisées avec le stock).
- IDs globalement uniques : chaque shard utilise auto_increment_offset = rang (1..4) et
  auto_increment_increment = 4  =>  shard_of_id(id) = SHARDS[(id - 1) % 4].
"""
import logging
import os
import threading
import time
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal

import mysql.connector
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from mysql.connector import Error as MySQLError
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("globalshop")

# --------------------------------------------------------------------------- configuration


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


DB_USER = env("DB_USER", "globalshop")
DB_PASSWORD = env("DB_PASSWORD", "globalpass")
DB_NAME = env("DB_NAME", "globalshop")
ROOT_USER = "root"
ROOT_PASSWORD = env("ROOT_PASSWORD", "rootpass")
REPL_USER = env("REPL_USER", "repl")
REPL_PASSWORD = env("REPL_PASSWORD", "replpass")

AUTO_FAILOVER = env("AUTO_FAILOVER", "true").lower() == "true"
FAILOVER_THRESHOLD = int(env("FAILOVER_THRESHOLD", "3"))  # sondes KO consécutives avant promotion
MONITOR_INTERVAL = float(env("MONITOR_INTERVAL", "1.0"))  # secondes entre deux cycles de supervision
PROBE_TIMEOUT = float(env("PROBE_TIMEOUT", "2"))  # timeout d'une sonde de santé
READ_TIMEOUT = float(env("READ_TIMEOUT", "5"))  # timeout des lectures
TX_TIMEOUT = float(env("TX_TIMEOUT", "20"))  # timeout des transactions (attente de verrou incluse)
MAX_ATTACH_ATTEMPTS = 3

SHARDS = ["africa", "europe", "america", "asia"]  # l'ordre définit le rang => offset auto-increment
N_SHARDS = len(SHARDS)

# --------------------------------------------------------------------------- état du cluster

STATE: dict = {}
EVENTS: deque = deque(maxlen=200)
STOP = threading.Event()


def new_state(name: str) -> dict:
    return {
        "primary": f"mysql-{name}-primary",
        "replica": f"mysql-{name}-replica",
        "primary_up": None,
        "replica_up": None,
        "status": "STARTING",  # STARTING | OK | DEGRADED | DOWN | NEEDS_RESYNC
        "failures": 0,
        "attach_attempts": 0,
        "failover_count": 0,
        "last_event": None,
        "last_error": None,
        "replication": None,
    }


for _name in SHARDS:
    STATE[_name] = new_state(_name)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def log_event(shard: str, message: str) -> None:
    EVENTS.append({"time": now_iso(), "shard": shard, "message": message})
    STATE[shard]["last_event"] = f"{now_iso()} {message}"
    log.warning("[%s] %s", shard, message)


# --------------------------------------------------------------------------- routage


def shard_of_id(entity_id: int) -> str:
    """Le shard propriétaire est encodé dans l'ID (auto_increment_offset)."""
    return SHARDS[(entity_id - 1) % N_SHARDS]


def product_shard(name: str) -> str:
    """Partitionnement des produits : hash stable (crc32) du nom normalisé."""
    return SHARDS[zlib.crc32(name.strip().lower().encode("utf-8")) % N_SHARDS]


def client_shard(region: str) -> str:
    region = region.strip().lower()
    if region not in SHARDS:
        raise HTTPException(400, f"Région inconnue '{region}'. Valeurs : {', '.join(SHARDS)}")
    return region


# --------------------------------------------------------------------------- connexions MySQL


def connect(host: str, timeout: float = READ_TIMEOUT):
    """Connexion applicative (compte globalshop) sur la base du shard."""
    return mysql.connector.connect(
        host=host, port=3306, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        connection_timeout=max(1, int(round(timeout))), autocommit=False,
    )


def connect_admin(host: str, timeout: float = PROBE_TIMEOUT):
    """Connexion root (supervision, réplication, promotion)."""
    return mysql.connector.connect(
        host=host, port=3306, user=ROOT_USER, password=ROOT_PASSWORD,
        connection_timeout=max(1, int(round(timeout))), autocommit=True,
    )


def close_quietly(conn) -> None:
    try:
        if conn is not None:
            conn.close()
    except Exception:  # noqa: BLE001
        pass


def unavailable(shard: str, exc: Exception, unknown_outcome: bool = False) -> HTTPException:
    detail = {
        "error": f"Shard '{shard}' indisponible (choix CP : on refuse plutôt que de risquer une incohérence).",
        "shard": shard,
        "cause": str(exc)[:200],
        "retry": True,
    }
    if unknown_outcome:
        detail["outcome"] = "unknown"  # la connexion a été perdue pendant l'écriture
    return HTTPException(503, detail=detail)


def write_connection(shard: str):
    """Les écritures vont UNIQUEMENT au primaire courant du shard."""
    host = STATE[shard]["primary"]
    try:
        return connect(host, timeout=TX_TIMEOUT)
    except MySQLError as exc:
        raise unavailable(shard, exc) from exc


def query_shard(shard: str, sql: str, params: tuple = ()):
    """Lecture : primaire d'abord ; repli sur la réplique si le primaire est injoignable
    (lecture potentiellement en retard - signalée via served_by='replica')."""
    s = STATE[shard]
    last_exc = None
    for role, host in (("primary", s["primary"]), ("replica", s["replica"])):
        conn = None
        try:
            conn = connect(host, timeout=READ_TIMEOUT)
            cur = conn.cursor(dictionary=True, buffered=True)
            cur.execute(sql, params)
            return cur.fetchall(), role
        except MySQLError as exc:
            last_exc = exc
            log.info("lecture %s sur %s impossible: %s", shard, host, exc)
        finally:
            close_quietly(conn)
    raise unavailable(shard, last_exc or Exception("aucun nœud joignable"))


def fan_out(sql: str, params: tuple = ()):
    """Lecture distribuée : interroge les 4 shards en parallèle."""

    def task(shard):
        try:
            rows, served = query_shard(shard, sql, params)
            return shard, rows, served, False
        except HTTPException:
            return shard, None, None, True

    rows_all, down, from_replica = [], [], []
    with ThreadPoolExecutor(max_workers=N_SHARDS) as ex:
        for shard, rows, served, failed in ex.map(task, SHARDS):
            if failed:
                down.append(shard)
                continue
            if served == "replica":
                from_replica.append(shard)
            for row in rows:
                row["node"] = shard
                rows_all.append(row)
    return rows_all, down, from_replica


# --------------------------------------------------------------------------- réplication / failover


def replica_status(host: str):
    """SHOW REPLICA STATUS du nœud (dict) ou None si non configuré / injoignable."""
    conn = None
    try:
        conn = connect_admin(host)
        cur = conn.cursor(dictionary=True, buffered=True)
        cur.execute("SHOW REPLICA STATUS")
        return cur.fetchone()
    except MySQLError:
        return None
    finally:
        close_quietly(conn)


def probe(host: str) -> bool:
    conn = None
    try:
        conn = connect_admin(host, timeout=PROBE_TIMEOUT)
        return True
    except Exception:  # noqa: BLE001
        return False
    finally:
        close_quietly(conn)


def set_read_only(host: str) -> None:
    conn = None
    try:
        conn = connect_admin(host)
        conn.cursor().execute("SET GLOBAL super_read_only = ON")
    except MySQLError:
        pass
    finally:
        close_quietly(conn)


def attach_replica(replica_host: str, source_host: str) -> None:
    """(Ré)configure `replica_host` comme réplique GTID de `source_host`.
    Verrouille d'abord le nœud en lecture seule (fencing : évite tout split-brain)."""
    conn = connect_admin(replica_host, timeout=5)
    try:
        cur = conn.cursor(buffered=True)
        cur.execute("SET GLOBAL super_read_only = ON")
        for stmt in ("STOP REPLICA", "RESET REPLICA ALL"):
            try:
                cur.execute(stmt)
            except MySQLError:
                pass
        cur.execute(
            "CHANGE REPLICATION SOURCE TO SOURCE_HOST=%s, SOURCE_PORT=3306, SOURCE_USER=%s, "
            "SOURCE_PASSWORD=%s, SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1",
            (source_host, REPL_USER, REPL_PASSWORD),
        )
        cur.execute("START REPLICA")
    finally:
        close_quietly(conn)


def promote(shard: str) -> bool:
    """Failover : promeut la réplique en nouveau primaire du shard."""
    s = STATE[shard]
    old_primary, new_primary = s["primary"], s["replica"]
    log_event(shard, f"FAILOVER: primaire {old_primary} injoignable -> promotion de {new_primary}")
    conn = None
    try:
        conn = connect_admin(new_primary, timeout=5)
        cur = conn.cursor(dictionary=True, buffered=True)
        # 1) cesser de recevoir, puis laisser le thread SQL appliquer tout le relay log
        try:
            cur.execute("STOP REPLICA IO_THREAD")
        except MySQLError:
            pass
        deadline = time.time() + 5
        while time.time() < deadline:
            cur.execute("SHOW REPLICA STATUS")
            st = cur.fetchone()
            if not st:
                break
            cur.execute(
                "SELECT GTID_SUBTRACT(%s, %s) AS pending",
                (st.get("Retrieved_Gtid_Set") or "", st.get("Executed_Gtid_Set") or ""),
            )
            if (cur.fetchone() or {}).get("pending", "") == "":
                break
            time.sleep(0.2)
        # 2) détacher définitivement de l'ancien primaire et ouvrir en écriture
        for stmt in ("STOP REPLICA", "RESET REPLICA ALL"):
            try:
                cur.execute(stmt)
            except MySQLError:
                pass
        cur.execute("SET GLOBAL super_read_only = OFF")
        cur.execute("SET GLOBAL read_only = OFF")
    except MySQLError as exc:
        log_event(shard, f"FAILOVER ÉCHOUÉ sur {new_primary}: {exc}")
        return False
    finally:
        close_quietly(conn)

    # 3) bascule des rôles côté routeur
    s["primary"], s["replica"] = new_primary, old_primary
    s["failures"] = 0
    s["attach_attempts"] = 0
    s["failover_count"] += 1
    s["status"] = "DEGRADED"
    s["replication"] = None
    log_event(shard, f"FAILOVER TERMINÉ: nouveau primaire = {new_primary} (écritures rouvertes)")
    return True


def reconcile_replica(shard: str) -> None:
    """Primaire ET réplique joignables : vérifie / répare la réplication."""
    s = STATE[shard]
    st = replica_status(s["replica"])

    if st is None or st.get("Source_Host") != s["primary"]:
        if s["attach_attempts"] >= MAX_ATTACH_ATTEMPTS:
            s["status"] = "NEEDS_RESYNC"
            return
        s["attach_attempts"] += 1
        try:
            attach_replica(s["replica"], s["primary"])
            log_event(shard, f"réplique {s['replica']} rattachée à {s['primary']} (tentative {s['attach_attempts']})")
        except MySQLError as exc:
            s["last_error"] = str(exc)[:300]
            log_event(shard, f"rattachement de {s['replica']} impossible: {exc}")
        s["status"] = "DEGRADED"
        return

    set_read_only(s["replica"])
    io_ok = st.get("Replica_IO_Running") == "Yes"
    sql_ok = st.get("Replica_SQL_Running") == "Yes"
    s["replication"] = {
        "source": st.get("Source_Host"),
        "io_running": st.get("Replica_IO_Running"),
        "sql_running": st.get("Replica_SQL_Running"),
        "lag_seconds": st.get("Seconds_Behind_Source"),
    }
    if io_ok and sql_ok:
        if s["status"] != "OK":
            log_event(shard, f"réplication saine ({s['replica']} <- {s['primary']})")
        s["status"] = "OK"
        s["attach_attempts"] = 0
        s["last_error"] = None
    elif st.get("Last_IO_Errno") or st.get("Last_SQL_Errno"):
        err = st.get("Last_IO_Error") or st.get("Last_SQL_Error") or "erreur de réplication"
        if s["status"] != "NEEDS_RESYNC":
            log_event(shard, f"RÉPLICATION EN ERREUR ({s['replica']}): {err[:200]} -> resynchronisation manuelle requise")
        s["status"] = "NEEDS_RESYNC"
        s["last_error"] = err[:300]
    else:
        s["status"] = "DEGRADED"  # rattrapage en cours


def check_shard(shard: str) -> None:
    s = STATE[shard]
    p_up, r_up = probe(s["primary"]), probe(s["replica"])
    s["primary_up"], s["replica_up"] = p_up, r_up

    if p_up:
        s["failures"] = 0
        if r_up:
            reconcile_replica(shard)
        else:
            if s["status"] != "DEGRADED":
                log_event(shard, f"réplique {s['replica']} indisponible (shard sans redondance)")
            s["status"] = "DEGRADED"
            s["replication"] = None
        return

    # primaire injoignable
    s["failures"] += 1
    s["status"] = "DOWN"
    if s["failures"] == 1:
        log_event(shard, f"primaire {s['primary']} injoignable (sonde 1/{FAILOVER_THRESHOLD})")
    if s["failures"] >= FAILOVER_THRESHOLD and AUTO_FAILOVER and r_up:
        promote(shard)


def safe_check(shard: str) -> None:
    try:
        check_shard(shard)
    except Exception:  # noqa: BLE001 - une erreur sur un shard ne doit pas bloquer les autres
        log.exception("erreur de supervision sur le shard %s", shard)


def discover_roles(shard: str) -> None:
    """Au démarrage du routeur : retrouve les rôles réels (un failover a pu avoir lieu avant
    un redémarrage du routeur). Un nœud configuré comme réplique de l'autre est la réplique."""
    s = STATE[shard]
    default_primary, default_replica = s["primary"], s["replica"]
    st = replica_status(default_primary)
    if st and st.get("Source_Host") == default_replica:
        s["primary"], s["replica"] = default_replica, default_primary
        log_event(shard, f"rôles détectés au démarrage: primaire = {s['primary']} (failover antérieur)")


def monitor_loop() -> None:
    for shard in SHARDS:
        discover_roles(shard)
    while not STOP.is_set():
        started = time.time()
        try:
            with ThreadPoolExecutor(max_workers=N_SHARDS) as ex:
                list(ex.map(safe_check, SHARDS))
        except Exception:  # noqa: BLE001
            log.exception("erreur dans le cycle de supervision")
        STOP.wait(max(0.0, MONITOR_INTERVAL - (time.time() - started)))


@asynccontextmanager
async def lifespan(_: FastAPI):
    threading.Thread(target=monitor_loop, name="monitor", daemon=True).start()
    yield
    STOP.set()


app = FastAPI(title="GlobalShop - base de données distribuée résiliente", lifespan=lifespan)

# --------------------------------------------------------------------------- modèles


class ClientCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(min_length=3, max_length=150)
    region: str


class ProductCreate(BaseModel):
    name: str = Field(min_length=1, max_length=150)
    description: str = ""
    price: float = Field(ge=0)
    stock: int = Field(ge=0)


class OrderCreate(BaseModel):
    client_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)


# --------------------------------------------------------------------------- supervision


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/health")
def health():
    shards = {name: s["status"] for name, s in STATE.items()}
    if all(v == "OK" for v in shards.values()):
        overall = "healthy"
    elif any(s["status"] == "DOWN" for s in STATE.values()):
        overall = "down"
    else:
        overall = "degraded"
    return {"status": overall, "shards": shards}


@app.get("/cluster")
def cluster():
    keys = ("primary", "replica", "primary_up", "replica_up", "status",
            "failover_count", "last_event", "last_error", "replication")
    return {name: {k: s[k] for k in keys} for name, s in STATE.items()}


@app.get("/events")
def events():
    return list(EVENTS)


@app.get("/route")
def route(name: str):
    """Montre vers quel shard un produit de ce nom serait placé (utile pour la démo)."""
    return {"name": name, "node": product_shard(name)}


# --------------------------------------------------------------------------- clients


def email_exists_anywhere(email: str) -> bool:
    """Unicité de l'email à l'échelle du cluster (au mieux : shards injoignables ignorés)."""
    for shard in SHARDS:
        try:
            rows, _ = query_shard(shard, "SELECT id FROM clients WHERE email=%s LIMIT 1", (email,))
            if rows:
                return True
        except HTTPException:
            continue
    return False


@app.post("/clients")
def create_client(client: ClientCreate):
    shard = client_shard(client.region)
    if email_exists_anywhere(client.email):
        raise HTTPException(409, "Cet email existe déjà.")
    conn = write_connection(shard)
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(
            "INSERT INTO clients (name, email, region) VALUES (%s, %s, %s)",
            (client.name, client.email, shard),
        )
        client_id = cur.lastrowid
        conn.commit()
        return {"client_id": client_id, "node": shard, "region": shard}
    except mysql.connector.IntegrityError as exc:
        raise HTTPException(409, "Cet email existe déjà.") from exc
    except MySQLError as exc:
        raise unavailable(shard, exc, unknown_outcome=True) from exc
    finally:
        close_quietly(conn)


@app.get("/clients")
def list_clients(limit: int = Query(100, ge=1, le=1000)):
    rows, down, from_replica = fan_out(
        "SELECT id, name, email, region, created_at FROM clients ORDER BY id DESC LIMIT %s", (limit,)
    )
    rows.sort(key=lambda r: r["created_at"], reverse=True)
    return {"clients": rows[:limit], "count": len(rows[:limit]), "partial": bool(down),
            "unavailable_shards": down, "served_by_replica": from_replica}


# --------------------------------------------------------------------------- produits


@app.post("/products")
def create_product(product: ProductCreate):
    shard = product_shard(product.name)
    price = Decimal(str(product.price)).quantize(Decimal("0.01"))
    conn = write_connection(shard)
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(
            "INSERT INTO products (name, description, price, stock) VALUES (%s, %s, %s, %s)",
            (product.name, product.description, price, product.stock),
        )
        product_id = cur.lastrowid
        conn.commit()
        return {"product_id": product_id, "node": shard}
    except MySQLError as exc:
        raise unavailable(shard, exc, unknown_outcome=True) from exc
    finally:
        close_quietly(conn)


@app.get("/products")
def list_products(limit: int = Query(200, ge=1, le=2000)):
    """Lecture distribuée du catalogue (scatter-gather sur les 4 shards)."""
    rows, down, from_replica = fan_out(
        "SELECT id, name, description, price, stock FROM products ORDER BY id DESC LIMIT %s", (limit,)
    )
    rows.sort(key=lambda r: r["id"], reverse=True)
    rows = rows[:limit]
    return {"products": rows, "count": len(rows), "partial": bool(down),
            "unavailable_shards": down, "served_by_replica": from_replica}


@app.get("/products/{product_id}")
def get_product(product_id: int):
    if product_id < 1:
        raise HTTPException(404, "Produit introuvable.")
    shard = shard_of_id(product_id)
    rows, served = query_shard(
        shard, "SELECT id, name, description, price, stock FROM products WHERE id=%s", (product_id,)
    )
    if not rows:
        raise HTTPException(404, "Produit introuvable.")
    return {**rows[0], "node": shard, "served_by": served}


@app.get("/products/{product_id}/audit")
def audit_product(product_id: int):
    """Contrôle d'intégrité : stock restant, nombre/unités de commandes et liste des IDs.
    Invariant attendu : stock_initial = stock + units_sold."""
    if product_id < 1:
        raise HTTPException(404, "Produit introuvable.")
    shard = shard_of_id(product_id)
    prod, served = query_shard(shard, "SELECT id, name, stock FROM products WHERE id=%s", (product_id,))
    if not prod:
        raise HTTPException(404, "Produit introuvable.")
    orders, _ = query_shard(shard, "SELECT id, quantity FROM orders WHERE product_id=%s ORDER BY id", (product_id,))
    return {
        "product_id": product_id,
        "node": shard,
        "served_by": served,
        "stock": prod[0]["stock"],
        "orders_count": len(orders),
        "units_sold": sum(int(o["quantity"]) for o in orders),
        "order_ids": [o["id"] for o in orders],
    }


# --------------------------------------------------------------------------- commandes


@app.post("/orders")
def create_order(order: OrderCreate):
    """Passage de commande.

    Anti-survente : la commande, la facture et le décrément de stock s'exécutent dans UNE
    transaction sur le primaire du shard propriétaire du produit. SELECT ... FOR UPDATE
    sérialise les acheteurs concurrents du même produit ; la clause `stock >= qte` et la
    contrainte CHECK (stock >= 0) sont deux barrières supplémentaires.
    Pas de transaction distribuée (2PC) : stock et commande sont colocalisés.
    """
    c_shard = shard_of_id(order.client_id)
    clients, _ = query_shard(c_shard, "SELECT id, region FROM clients WHERE id=%s", (order.client_id,))
    if not clients:
        raise HTTPException(404, "Client introuvable.")

    p_shard = shard_of_id(order.product_id)
    conn = write_connection(p_shard)
    try:
        cur = conn.cursor(dictionary=True, buffered=True)
        conn.start_transaction()
        cur.execute("SELECT id, name, price, stock FROM products WHERE id=%s FOR UPDATE", (order.product_id,))
        product = cur.fetchone()
        if product is None:
            conn.rollback()
            raise HTTPException(404, "Produit introuvable.")
        if product["stock"] < order.quantity:
            conn.rollback()
            raise HTTPException(409, f"Stock insuffisant (restant : {product['stock']}).")

        total = product["price"] * order.quantity
        cur.execute(
            "UPDATE products SET stock = stock - %s WHERE id=%s AND stock >= %s",
            (order.quantity, order.product_id, order.quantity),
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise HTTPException(409, "Stock insuffisant.")
        cur.execute(
            "INSERT INTO orders (client_id, product_id, quantity, total_amount, status) "
            "VALUES (%s, %s, %s, %s, 'CONFIRMED')",
            (order.client_id, order.product_id, order.quantity, total),
        )
        order_id = cur.lastrowid
        cur.execute(
            "INSERT INTO invoices (order_id, amount, status) VALUES (%s, %s, 'PAID')", (order_id, total)
        )
        invoice_id = cur.lastrowid
        conn.commit()
        return {
            "order_id": order_id,
            "invoice_id": invoice_id,
            "client_id": order.client_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "total": float(total),
            "remaining_stock": product["stock"] - order.quantity,
            "client_node": c_shard,
            "product_node": p_shard,
        }
    except HTTPException:
        raise
    except MySQLError as exc:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        raise unavailable(p_shard, exc, unknown_outcome=True) from exc
    finally:
        close_quietly(conn)


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    if order_id < 1:
        raise HTTPException(404, "Commande introuvable.")
    shard = shard_of_id(order_id)
    rows, served = query_shard(
        shard,
        "SELECT o.id, o.client_id, o.product_id, o.quantity, o.total_amount, o.status, o.created_at, "
        "i.id AS invoice_id, i.amount AS invoice_amount, i.status AS invoice_status "
        "FROM orders o LEFT JOIN invoices i ON i.order_id = o.id WHERE o.id=%s",
        (order_id,),
    )
    if not rows:
        raise HTTPException(404, "Commande introuvable.")
    return {**rows[0], "node": shard, "served_by": served}


@app.get("/orders")
def list_orders(limit: int = Query(50, ge=1, le=1000)):
    rows, down, from_replica = fan_out(
        "SELECT id, client_id, product_id, quantity, total_amount, status, created_at "
        "FROM orders ORDER BY id DESC LIMIT %s",
        (limit,),
    )
    rows.sort(key=lambda r: r["created_at"], reverse=True)
    rows = rows[:limit]
    return {"orders": rows, "count": len(rows), "partial": bool(down),
            "unavailable_shards": down, "served_by_replica": from_replica}
