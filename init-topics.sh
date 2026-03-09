#!/bin/bash
# ============================================================
# init-topics.sh
# Création explicite des topics Kafka pour le projet
# Events-driven-project
#
# À exécuter UNE SEULE FOIS après docker-compose up -d
# Usage : ./init-topics.sh
# ============================================================

set -e  # Arrête le script immédiatement si une commande échoue
        # Sans ça, le script continue même en cas d'erreur silencieuse

# ============================================================
# CONFIGURATION
# ============================================================
KAFKA_CONTAINER="kafka-1"
BOOTSTRAP_SERVER="localhost:29092,localhost:29093,localhost:29094"
REPLICATION_FACTOR=3
RETENTION_MS=259200000                   # 3 jours de rétention (3 * 24 * 60 * 60 * 1000)
                                         # Bronze layer = on garde l'historique

# ============================================================
# HELPERS
# ============================================================

# Fonction qui exécute une commande kafka DANS le container
kafka_cmd() {
    docker exec "$KAFKA_CONTAINER" /bin/kafka-topics \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        "$@"
}

# Fonction qui crée un topic avec les bons paramètres
# Si le topic existe déjà, elle ne fait rien (idempotent)
create_topic() {
    local topic=$1
    local partitions=$2
    local description=$3

    echo "📌 Création du topic : $topic"
    echo "   Partitions        : $partitions"
    echo "   Replication       : $REPLICATION_FACTOR"
    echo "   Rétention         : 3 jours"
    echo "   Description       : $description"

    # --if-not-exists : idempotent — pas d'erreur si le topic existe déjà
    # Utile pour rejouer le script sans tout casser
    kafka_cmd --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$REPLICATION_FACTOR" \
        --if-not-exists \
        --config retention.ms="$RETENTION_MS" \
        --config cleanup.policy=delete

    # cleanup.policy=delete → les messages sont supprimés après retention.ms
    # Alternative : compact → Kafka garde le dernier message par clé (pas adapté ici)

    echo "   ✅ OK"
    echo ""
}

# ============================================================
# ATTENTE QUE KAFKA SOIT PRÊT
# ============================================================
echo "⏳ Attente que Kafka soit prêt..."

MAX_RETRIES=10
RETRY=0

until docker exec "$KAFKA_CONTAINER" /bin/kafka-topics \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list > /dev/null 2>&1; do

    RETRY=$((RETRY + 1))

    if [ "$RETRY" -ge "$MAX_RETRIES" ]; then
        echo "❌ Kafka n'est pas disponible après $MAX_RETRIES tentatives"
        echo "   → Vérifie que Docker est lancé : docker-compose up -d"
        exit 1
    fi

    echo "   tentative $RETRY/$MAX_RETRIES — nouvelle tentative dans 3s..."
    sleep 3
done

echo "✅ Kafka est prêt"
echo ""

# ============================================================
# CRÉATION DES TOPICS
# ============================================================
echo "============================================"
echo " Création des topics — Events-driven-project"
echo "============================================"
echo ""

# ----------------------------------------------------------
# transit.raw.passages
# Prochains passages aux arrêts (API stop-monitoring PRIM)
# Fréquence : toutes les 60s
# Volume estimé : ~1440 messages/jour
#
# Partitions : 7
# Mapping partition → line_id PRIM validé (Mars 2026) :
#
#   Partition 0  →  rer-a       (STIF:Line::C01371:)
#   Partition 1  →  rer-b       (STIF:Line::C01372:)
#   Partition 2  →  rer-c       (STIF:Line::C01373:)
#   Partition 3  →  rer-d       (STIF:Line::C01616:)
#   Partition 4  →  rer-e       (STIF:Line::C01374:)
#   Partition 5  →  metro       (toutes lignes métro)
#   Partition 6  →  bus-tramway (toutes lignes bus/tram)
#
# La partition key est calculée dans producer.py via get_partition_key()
# Kafka hash la clé et la mappe sur la partition correspondante
# → tous les events d'une même ligne = même partition = ordre garanti
# ----------------------------------------------------------
create_topic \
    "transit.raw.passages" \
    7 \
    "Prochains passages aux arrêts - SIRI brut"

# ----------------------------------------------------------
# transit.raw.alerts
# Messages info trafic et perturbations (API general-message PRIM)
# Fréquence : toutes les 5min
# Volume estimé : ~288 messages/jour
#
# Partitions : 1
# Les alertes sont moins volumineuses et moins critiques
# 1 partition suffit largement
# ----------------------------------------------------------
create_topic \
    "transit.raw.alerts" \
    1 \
    "Perturbations et info trafic - SIRI brut"

# ============================================================
# VÉRIFICATION
# ============================================================
echo "============================================"
echo " Vérification des topics créés"
echo "============================================"
echo ""

kafka_cmd --describe --topic "transit.raw.passages"
echo ""
kafka_cmd --describe --topic "transit.raw.alerts"
echo ""

echo "============================================"
echo " Initialisation terminée"
echo " → Tu peux maintenant lancer le producer"
echo " → make run  ou  python producer.py"
echo "============================================"
