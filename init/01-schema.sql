-- =====================================================================
-- GlobalShop - schéma d'un shard (exécuté à l'identique sur CHAQUE nœud,
-- primaire comme réplique).
--
-- IMPORTANT : tout ce fichier s'exécute avec sql_log_bin = 0.
-- Les nœuds démarrent ainsi avec un état initial identique et SANS
-- transaction GTID locale ("errante"). La réplication ne transporte donc
-- que les données applicatives écrites ensuite par le routeur.
-- =====================================================================
SET SESSION sql_log_bin = 0;

CREATE DATABASE IF NOT EXISTS globalshop CHARACTER SET utf8mb4;

-- Compte applicatif (routeur) et compte de réplication
CREATE USER IF NOT EXISTS 'globalshop'@'%' IDENTIFIED BY 'globalpass';
GRANT ALL PRIVILEGES ON globalshop.* TO 'globalshop'@'%';
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'replpass';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;

USE globalshop;

-- Clients : fragmentation horizontale par RÉGION (placement géographique)
CREATE TABLE IF NOT EXISTS clients (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(100) NOT NULL,
    email      VARCHAR(150) NOT NULL,
    region     VARCHAR(30)  NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_clients_email (email)
) ENGINE=InnoDB;

-- Produits : fragmentation horizontale par HASH du nom (crc32 % 4)
-- La contrainte CHECK est une 2e barrière anti-survente, au niveau SGBD.
CREATE TABLE IF NOT EXISTS products (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(150)  NOT NULL,
    description TEXT,
    price       DECIMAL(12,2) NOT NULL,
    stock       INT           NOT NULL DEFAULT 0,
    created_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_products_stock CHECK (stock >= 0),
    INDEX idx_products_name (name)
) ENGINE=InnoDB;

-- Commandes : stockées sur le shard du PRODUIT (colocalisation avec le stock)
-- => décrément de stock + commande + facture = UNE transaction locale ACID.
-- client_id référence un client d'un autre shard : pas de FK inter-nœuds.
CREATE TABLE IF NOT EXISTS orders (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    client_id    INT           NOT NULL,
    product_id   INT           NOT NULL,
    quantity     INT           NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    status       VARCHAR(30)   NOT NULL DEFAULT 'CONFIRMED',
    created_at   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_orders_client (client_id),
    INDEX idx_orders_product (product_id),
    CONSTRAINT fk_orders_product FOREIGN KEY (product_id) REFERENCES products(id)
) ENGINE=InnoDB;

-- Factures : même shard que leur commande
CREATE TABLE IF NOT EXISTS invoices (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    order_id   INT           NOT NULL,
    amount     DECIMAL(12,2) NOT NULL,
    status     VARCHAR(30)   NOT NULL DEFAULT 'PAID',
    issued_at  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_invoices_order (order_id),
    CONSTRAINT fk_invoices_order FOREIGN KEY (order_id) REFERENCES orders(id)
) ENGINE=InnoDB;
