-- monitoring/postgres/init.sql
-- Initialisation de la base de données WaveGuard

-- ============================================================
-- TABLE : Transactions
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    sender_id VARCHAR(20) NOT NULL,
    receiver_id VARCHAR(20) NOT NULL,
    amount_fcfa DECIMAL(15,2) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    location VARCHAR(50) NOT NULL,
    is_flagged BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- TABLE : Alertes de fraude
-- ============================================================
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    sender_id VARCHAR(20) NOT NULL,
    fraud_type VARCHAR(30) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    metric_value DECIMAL(15,2) NOT NULL,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    risk_level VARCHAR(20) DEFAULT 'HIGH'
);

-- ============================================================
-- TABLE : Métriques agrégées (pour Grafana)
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics_aggregates (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(15,2) NOT NULL,
    metric_type VARCHAR(20) NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEXES pour les performances
-- ============================================================
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX idx_transactions_sender_id ON transactions(sender_id);
CREATE INDEX idx_fraud_alerts_detected_at ON fraud_alerts(detected_at);
CREATE INDEX idx_fraud_alerts_sender_id ON fraud_alerts(sender_id);
CREATE INDEX idx_metrics_recorded_at ON metrics_aggregates(recorded_at);

-- ============================================================
-- VUES pour Grafana
-- ============================================================

-- 1. Vue : Statistiques quotidiennes des transactions
CREATE OR REPLACE VIEW daily_transaction_stats AS
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as total_transactions,
    SUM(amount_fcfa) as total_amount,
    AVG(amount_fcfa) as avg_amount,
    COUNT(CASE WHEN is_flagged THEN 1 END) as flagged_transactions
FROM transactions
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- 2. Vue : Top fraudeurs
CREATE OR REPLACE VIEW top_fraudsters AS
SELECT 
    sender_id,
    COUNT(*) as alert_count,
    MAX(risk_level) as max_risk_level,
    MIN(detected_at) as first_alert,
    MAX(detected_at) as last_alert
FROM fraud_alerts
GROUP BY sender_id
ORDER BY alert_count DESC
LIMIT 10;

-- 3. Vue : Alertes par type et jour
CREATE OR REPLACE VIEW alerts_by_type_day AS
SELECT 
    DATE(detected_at) as date,
    fraud_type,
    COUNT(*) as count
FROM fraud_alerts
GROUP BY DATE(detected_at), fraud_type
ORDER BY date DESC, fraud_type;

-- ============================================================
-- DONNÉES DE TEST (optionnel)
-- ============================================================
-- Insérer des données de test pour Grafana
INSERT INTO metrics_aggregates (metric_name, metric_value, metric_type) VALUES
    ('fraud_alerts_total', 0, 'counter'),
    ('transactions_total', 0, 'counter'),
    ('alert_rate', 0, 'gauge'),
    ('fraud_amount_total', 0, 'counter');