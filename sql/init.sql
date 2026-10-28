-- Schéma HealthStream ICU Monitor — séance 2
-- Exécuté une seule fois, au premier démarrage du volume pg-data
-- (/var/lib/postgres/initdb/01-init.sql dans l'image Chainguard).

CREATE TABLE IF NOT EXISTS vitals_raw (
    kafka_partition  INTEGER       NOT NULL,
    kafka_offset     BIGINT        NOT NULL,
    patient_id       VARCHAR(16)   NOT NULL,
    event_time       TIMESTAMPTZ   NOT NULL,
    vital_type       VARCHAR(16)   NOT NULL,
    value            NUMERIC(8, 2) NOT NULL,
    unit             VARCHAR(8)    NOT NULL,
    ingested_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- Rejouer le même message Kafka ne peut pas créer une seconde ligne :
    -- (partition, offset) identifie un message de façon unique et stable.
    CONSTRAINT pk_vitals_raw PRIMARY KEY (kafka_partition, kafka_offset),
    CONSTRAINT ck_vital_type CHECK (vital_type IN ('HR', 'SPO2', 'ABP_SYS'))
);

-- Requête dominante : « la série d'un patient sur une fenêtre récente ».
CREATE INDEX IF NOT EXISTS idx_vitals_patient_time
    ON vitals_raw (patient_id, event_time DESC);

-- Vue de contrôle : lignes_totales = messages_distincts <=> aucun doublon.
CREATE OR REPLACE VIEW v_vitals_controle AS
SELECT
    COUNT(*)                                          AS lignes_totales,
    COUNT(DISTINCT (kafka_partition, kafka_offset))   AS messages_distincts,
    COUNT(DISTINCT patient_id)                        AS patients_distincts,
    MIN(event_time)                                   AS premier_releve,
    MAX(event_time)                                   AS dernier_releve
FROM vitals_raw;
