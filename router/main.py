from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import mysql.connector

app = FastAPI(
    title="GlobalShop Distributed API",
    version="1.0.0",
    description="API de coordination pour une base de données e-commerce distribuée."
)

NODES = {
    "africa": {
        "host": "mysql-africa",
        "port": 3306,
        "database": "globalshop",
        "user": "globalshop",
        "password": "globalpass",
    },
    "europe": {
        "host": "mysql-europe",
        "port": 3306,
        "database": "globalshop",
        "user": "globalshop",
        "password": "globalpass",
    },
    "america": {
        "host": "mysql-america",
        "port": 3306,
        "database": "globalshop",
        "user": "globalshop",
        "password": "globalpass",
    },
    "asia": {
        "host": "mysql-asia",
        "port": 3306,
        "database": "globalshop",
        "user": "globalshop",
        "password": "globalpass",
    },
}


def get_connection(node_name: str):
    if node_name not in NODES:
        raise ValueError(f"Nœud inconnu: {node_name}")
    return mysql.connector.connect(**NODES[node_name])


def choose_product_node(name: str) -> str:
    index = sum(ord(c) for c in name) % len(NODES)
    return list(NODES.keys())[index]


class ClientCreate(BaseModel):
    name: str
    email: str
    region: str


class ProductCreate(BaseModel):
    name: str
    description: str
    price: float
    stock: int


class OrderCreate(BaseModel):
    client_id: int
    product_id: int
    quantity: int


@app.get("/", include_in_schema=False)
def root():
    return FileResponse("static/index.html")


@app.get("/health")
def health():
    result = {}

    for node_name in NODES:
        try:
            connection = get_connection(node_name)
            connection.close()
            result[node_name] = "UP"
        except Exception:
            result[node_name] = "DOWN"

    return result


@app.get("/nodes/status")
def nodes_status():
    result = {}

    for node_name, config in NODES.items():
        try:
            connection = get_connection(node_name)
            connection.close()
            result[node_name] = {
                "status": "UP",
                "host": config["host"],
                "port": config["port"],
            }
        except Exception as exc:
            result[node_name] = {
                "status": "DOWN",
                "host": config["host"],
                "port": config["port"],
                "error": str(exc),
            }

    return result


@app.post("/clients")
def create_client(client: ClientCreate):
    region = client.region.lower()

    if region not in NODES:
        raise HTTPException(
            status_code=400,
            detail="Région invalide. Utilisez africa, europe, america ou asia."
        )

    connection = None
    cursor = None

    try:
        connection = get_connection(region)
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO clients (name, email, region)
            VALUES (%s, %s, %s)
            """,
            (client.name, client.email, region),
        )

        client_id = cursor.lastrowid
        connection.commit()

        return {
            "message": "Client créé avec succès",
            "client_id": client_id,
            "node": region,
            "region": region,
        }

    except mysql.connector.IntegrityError:
        if connection:
            connection.rollback()
        raise HTTPException(
            status_code=409,
            detail="Un client avec cet email existe déjà."
        )

    except Exception as exc:
        if connection:
            connection.rollback()
        raise HTTPException(status_code=500, detail=str(exc))

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@app.get("/clients")
def get_clients():
    clients = []

    for node_name in NODES:
        connection = None
        cursor = None

        try:
            connection = get_connection(node_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT id, name, email, region, created_at
                FROM clients
                ORDER BY id DESC
                """
            )

            for row in cursor.fetchall():
                row["node"] = node_name
                clients.append(row)

        except Exception:
            # Un nœud indisponible ne bloque pas l'affichage des autres.
            continue

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    return {"clients": clients}


@app.post("/products")
def create_product(product: ProductCreate):
    if product.price < 0:
        raise HTTPException(status_code=400, detail="Le prix doit être positif.")

    if product.stock < 0:
        raise HTTPException(status_code=400, detail="Le stock doit être positif.")

    node_name = choose_product_node(product.name)

    connection = None
    cursor = None

    try:
        connection = get_connection(node_name)
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO products (name, description, price, stock)
            VALUES (%s, %s, %s, %s)
            """,
            (
                product.name,
                product.description,
                product.price,
                product.stock,
            ),
        )

        product_id = cursor.lastrowid
        connection.commit()

        return {
            "message": "Produit créé avec succès",
            "product_id": product_id,
            "node": node_name,
            "stock": product.stock,
        }

    except Exception as exc:
        if connection:
            connection.rollback()
        raise HTTPException(status_code=500, detail=str(exc))

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


@app.get("/products")
def get_products():
    products = []

    for node_name in NODES:
        connection = None
        cursor = None

        try:
            connection = get_connection(node_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT id, name, description, price, stock, created_at
                FROM products
                ORDER BY id DESC
                """
            )

            for row in cursor.fetchall():
                row["node"] = node_name
                products.append(row)

        except Exception:
            continue

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    return {"products": products}


@app.get("/products/{product_id}")
def get_product(product_id: int):
    for node_name in NODES:
        connection = None
        cursor = None

        try:
            connection = get_connection(node_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT id, name, description, price, stock, created_at
                FROM products
                WHERE id = %s
                """,
                (product_id,),
            )

            product = cursor.fetchone()

            if product:
                product["node"] = node_name
                return product

        except Exception:
            continue

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    raise HTTPException(status_code=404, detail="Produit introuvable.")


@app.post("/orders")
def create_order(order: OrderCreate):
    if order.quantity <= 0:
        raise HTTPException(
            status_code=400,
            detail="La quantité doit être supérieure à zéro."
        )

    client_node = None
    product_node = None

    # Recherche du client dans les quatre partitions.
    for node_name in NODES:
        connection = None
        cursor = None

        try:
            connection = get_connection(node_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(
                "SELECT id, region FROM clients WHERE id = %s",
                (order.client_id,),
            )

            client = cursor.fetchone()

            if client:
                client_node = node_name
                break

        except Exception:
            continue

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    if client_node is None:
        raise HTTPException(status_code=404, detail="Client introuvable.")

    # Recherche du produit dans les quatre partitions.
    for node_name in NODES:
        connection = None
        cursor = None

        try:
            connection = get_connection(node_name)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(
                """
                SELECT id, name, price, stock
                FROM products
                WHERE id = %s
                """,
                (order.product_id,),
            )

            product = cursor.fetchone()

            if product:
                product_node = node_name
                break

        except Exception:
            continue

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    if product_node is None:
        raise HTTPException(status_code=404, detail="Produit introuvable.")

    # La transaction critique est exécutée sur le nœud propriétaire du produit.
    connection = None
    cursor = None

    try:
        connection = get_connection(product_node)
        connection.start_transaction()

        cursor = connection.cursor(dictionary=True)

        # Verrouillage pessimiste : deux achats simultanés
        # ne peuvent pas décrémenter le même stock en même temps.
        cursor.execute(
            """
            SELECT id, name, price, stock
            FROM products
            WHERE id = %s
            FOR UPDATE
            """,
            (order.product_id,),
        )

        product = cursor.fetchone()

        if not product:
            connection.rollback()
            raise HTTPException(status_code=404, detail="Produit introuvable.")

        if product["stock"] < order.quantity:
            connection.rollback()
            raise HTTPException(
                status_code=409,
                detail=f"Stock insuffisant. Stock disponible: {product['stock']}"
            )

        total = float(product["price"]) * order.quantity
        remaining_stock = product["stock"] - order.quantity

        cursor.execute(
            """
            UPDATE products
            SET stock = stock - %s
            WHERE id = %s
            """,
            (order.quantity, order.product_id),
        )

        cursor.execute(
            """
            INSERT INTO orders (client_id, total_amount, status)
            VALUES (%s, %s, %s)
            """,
            (order.client_id, total, "CONFIRMED"),
        )

        order_id = cursor.lastrowid

        cursor.execute(
            """
            INSERT INTO invoices (order_id, amount, status)
            VALUES (%s, %s, %s)
            """,
            (order_id, total, "PAID"),
        )

        invoice_id = cursor.lastrowid

        connection.commit()

        return {
            "message": "Commande confirmée",
            "order_id": order_id,
            "invoice_id": invoice_id,
            "client_id": order.client_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "total": total,
            "product_node": product_node,
            "client_node": client_node,
            "remaining_stock": remaining_stock,
        }

    except HTTPException:
        raise

    except Exception as exc:
        if connection:
            connection.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erreur transactionnelle: {exc}"
        )

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
