import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

API = "http://127.0.0.1:8000"

# ============================================================
# CONFIGURATION
# ============================================================

STOCK_INITIAL = 20
NOMBRE_ACHATS = 100

# ============================================================
# 1. CREATION DU PRODUIT
# ============================================================

product_data = {
    "name": "Produit Concurrency Test",
    "description": "Test de 100 achats simultanes",
    "price": 100,
    "stock": STOCK_INITIAL
}

response = requests.post(
    f"{API}/products",
    json=product_data
)

if response.status_code != 200:
    print("Erreur creation produit :", response.status_code)
    print(response.text)
    exit()

product = response.json()
product_id = product["product_id"]

print("=" * 60)
print("PRODUIT CREE")
print("=" * 60)
print("Product ID :", product_id)
print("Stock initial :", STOCK_INITIAL)
print()

# ============================================================
# 2. CREATION DU CLIENT
# ============================================================

client_data = {
    "name": "Client Concurrency Test",
    "email": f"concurrency{product_id}@test.com",
    "region": "africa"
}

response = requests.post(
    f"{API}/clients",
    json=client_data
)

if response.status_code != 200:
    print("Erreur creation client :", response.status_code)
    print(response.text)
    exit()

client = response.json()
client_id = client["client_id"]

print("CLIENT CREE")
print("Client ID :", client_id)
print()

# ============================================================
# 3. FONCTION D'ACHAT
# ============================================================

def acheter(numero):
    order_data = {
        "client_id": client_id,
        "product_id": product_id,
        "quantity": 1
    }

    try:
        response = requests.post(
            f"{API}/orders",
            json=order_data,
            timeout=10
        )

        return {
            "numero": numero,
            "status": response.status_code,
            "data": response.json()
        }

    except Exception as e:
        return {
            "numero": numero,
            "status": 0,
            "data": {"error": str(e)}
        }


# ============================================================
# 4. 100 ACHATS SIMULTANES
# ============================================================

print("=" * 60)
print("LANCEMENT DE 100 ACHATS SIMULTANES")
print("=" * 60)

resultats = []

with ThreadPoolExecutor(max_workers=100) as executor:

    futures = [
        executor.submit(acheter, i)
        for i in range(1, NOMBRE_ACHATS + 1)
    ]

    for future in as_completed(futures):
        resultats.append(future.result())


# ============================================================
# 5. ANALYSE DES RESULTATS
# ============================================================

succes = [
    r for r in resultats
    if r["status"] == 200
]

stock_insuffisant = [
    r for r in resultats
    if r["status"] == 409
]

erreurs = [
    r for r in resultats
    if r["status"] not in [200, 409]
]

print()
print("=" * 60)
print("RESULTATS")
print("=" * 60)

print("Nombre total de requetes :", len(resultats))
print("Achats acceptes          :", len(succes))
print("Stock insuffisant (409)  :", len(stock_insuffisant))
print("Autres erreurs           :", len(erreurs))

# ============================================================
# 6. VERIFICATION DU STOCK FINAL
# ============================================================

response = requests.get(
    f"{API}/products/{product_id}"
)

print()
print("=" * 60)
print("VERIFICATION DU STOCK")
print("=" * 60)

if response.status_code == 200:

    produit_final = response.json()
    stock_final = produit_final["stock"]

    print("Stock initial :", STOCK_INITIAL)
    print("Achats acceptes :", len(succes))
    print("Stock final :", stock_final)

    stock_attendu = STOCK_INITIAL - len(succes)

    print("Stock attendu :", stock_attendu)

    print()

    if stock_final == stock_attendu:
        print("✅ TEST REUSSI : aucune vente en trop.")
    else:
        print("❌ TEST ECHOUE : incoherence du stock.")

else:
    print("Impossible de recuperer le produit.")
    print(response.text)

# ============================================================
# 7. VERIFICATION ANTI-SURVENTE
# ============================================================

print()
print("=" * 60)

if len(succes) <= STOCK_INITIAL:
    print("✅ ANTI-SURVENTE : OK")
else:
    print("❌ ANTI-SURVENTE : ECHEC")

print("=" * 60)