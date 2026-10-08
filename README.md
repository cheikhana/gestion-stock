# GlobalShop — Base de données distribuée résiliente

Prototype académique de base de données distribuée pour une plateforme e-commerce internationale.

## 1. Architecture

GlobalShop utilise quatre nœuds MySQL indépendants et un middleware FastAPI.

- Node Africa : MySQL
- Node Europe : MySQL
- Node America : MySQL
- Node Asia : MySQL
- Router : FastAPI / coordinateur

Le Router masque la distribution aux clients et choisit le nœud responsable.

## 2. Partitionnement

### Clients
Partitionnement géographique :
- Africa → Node Africa
- Europe → Node Europe
- America → Node America
- Asia → Node Asia

### Produits
Distribution par hachage logique du nom du produit afin de répartir le catalogue entre les quatre nœuds.

### Commandes et factures
Elles sont enregistrées sur le nœud propriétaire du produit afin de garder la mise à jour du stock et la création de la commande dans la même transaction locale.

## 3. Cohérence et CAP

Le choix est **CP** pour les opérations critiques.

Pour un achat, il est préférable de refuser une opération lorsque la donnée de stock n'est pas disponible plutôt que d'autoriser une vente incohérente.

La concurrence est contrôlée avec une transaction MySQL et :

```sql
SELECT ... FOR UPDATE
```

Cela verrouille la ligne du produit pendant la modification du stock et empêche deux transactions concurrentes de vendre le même dernier article.

## 4. Docker

Démarrage complet :

```powershell
docker compose up -d --build
```

Arrêt :

```powershell
docker compose down
```

État :

```powershell
docker ps
```

API :

http://127.0.0.1:8000/docs

## 5. Vérification

Santé des nœuds :

```text
GET /health
```

Catalogue distribué :

```text
GET /products
```

Création client :

```text
POST /clients
```

Création produit :

```text
POST /products
```

Commande :

```text
POST /orders
```

## 6. Test de concurrence

Installer les dépendances du script :

```powershell
pip install requests
```

Puis :

```powershell
python scripts/test_demo.py
```

Le script crée un produit avec 20 unités puis lance 100 achats simultanés.

Résultat attendu :

- 20 achats acceptés maximum ;
- les autres demandes reçoivent HTTP 409 ;
- stock final = 0 ;
- aucune survente.

## 7. Test de panne

Le test arrête :

```text
globalshop-mysql-europe
```

Puis vérifie `/health`.

Europe doit passer à `DOWN` tandis que les autres nœuds restent `UP`.

Le conteneur est ensuite redémarré :

```powershell
docker start globalshop-mysql-europe
```

Après récupération, `/health` doit indiquer `UP`.

## 8. Limites du prototype

Le prototype utilise quatre bases MySQL indépendantes. La réplication physique MySQL maître/réplique et le failover automatique ne sont pas déployés dans cette version académique.

La résilience démontrée repose sur la détection de panne par le coordinateur, l'isolation des nœuds et la persistance Docker.

Pour les opérations critiques, le prototype privilégie la cohérence (CP).

## 9. Structure

```text
GlobalShop/
├── docker-compose.yml
├── init/
│   └── schema.sql
├── router/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
└── scripts/
    └── test_demo.py
```
