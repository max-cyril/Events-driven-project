import json
import logging
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

import config
from prim_client import PrimClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
# MAPPING LIGNES → PARTITION KEY
# ============================================================
# Line IDs validés depuis l'API PRIM (Mars 2026)
# Chaque line_id PRIM est mappé vers une partition key lisible
# Kafka hash cette clé et la mappe sur la bonne partition :
#
#   "rer-a"       → Partition 0
#   "rer-b"       → Partition 1
#   "rer-c"       → Partition 2
#   "rer-d"       → Partition 3
#   "rer-e"       → Partition 4
#   "metro"       → Partition 5
#   "bus-tramway" → Partition 6

RER_LINE_MAPPING = {
    "STIF:Line::C01371:": "rer-a",
    "STIF:Line::C01372:": "rer-b",
    "STIF:Line::C01373:": "rer-c",
    "STIF:Line::C01616:": "rer-d",
    "STIF:Line::C01374:": "rer-e",
}

# Préfixes des line_id PRIM pour identifier le mode de transport
# On les déduit de l'observation des données réelles
METRO_PREFIX = "STIF:Line::C0138"   # ex: C01382 = Métro 12
TRAM_PREFIX  = "STIF:Line::C0139"   # ex: C01390 = Tram T1


def get_partition_key(line_ref: str) -> str:
    """
    Mappe un line_ref PRIM complet vers une partition key lisible.

    Logique de priorité :
    1. C'est un RER connu → partition dédiée (rer-a à rer-e)
    2. C'est un Métro     → partition "metro"
    3. C'est un Tram      → partition "bus-tramway"
    4. Inconnu            → partition "bus-tramway" par défaut

    Args:
        line_ref : ex "STIF:Line::C01371:"

    Returns:
        str : partition key ex "rer-a", "metro", "bus-tramway"
    """
    # 1. RER — mapping exact
    if line_ref in RER_LINE_MAPPING:
        return RER_LINE_MAPPING[line_ref]

    # 2. Métro — par préfixe
    if METRO_PREFIX in line_ref:
        return "metro"

    # 3. Tram — par préfixe
    if TRAM_PREFIX in line_ref:
        return "bus-tramway"

    # 4. Fallback — bus ou ligne inconnue
    logger.debug(f"⚠️ line_ref inconnu → bus-tramway par défaut : {line_ref}")
    return "bus-tramway"


# ============================================================
# ARRÊTS À INTERROGER
# ============================================================
# Pour couvrir toutes les lignes on interroge un arrêt représentatif
# par mode de transport.
#
# Format : {"name": "...", "ref": "STIF:StopArea:SP:XXXXX:"}
#
# Stratégie :
# - 1 arrêt par ligne RER (validés précédemment)
# - 1 arrêt métro central (couvre plusieurs lignes)
# - 1 arrêt bus central
#
# On interroge tous ces arrêts à chaque cycle et on envoie
# chaque réponse dans la bonne partition via get_partition_key()

STOPS = [
    # RER — arrêts validés
    {"name": "Saint-Michel (RER A/B/C)", "ref": "STIF:StopArea:SP:22104:"},
    {"name": "Auber (RER A)",            "ref": "STIF:StopArea:SP:22123:"},
    {"name": "Nation (RER C)",           "ref": "STIF:StopArea:SP:22034:"},
    {"name": "Villiers-le-Bel (RER D)",  "ref": "STIF:StopArea:SP:60640:"},
    {"name": "Haussmann (RER E)",        "ref": "STIF:StopArea:SP:22144:"},

    # Métro
    {"name": "Trinité (Métro 12)",       "ref": "STIF:StopArea:SP:22046:"},

    # Bus — à valider (arrêt Opéra)
    {"name": "Opéra (Bus)",              "ref": "STIF:StopArea:SP:22109:"},
]


# ============================================================
# ENVELOPPE BRONZE
# ============================================================

def build_envelope(payload: dict, source: str, stop_name: str) -> tuple[dict, str]:
    """
    Construit l'enveloppe Bronze + calcule la partition key.

    On extrait le premier line_ref trouvé dans le payload
    pour déterminer la partition key.

    Returns:
        tuple[dict, str] : (enveloppe Bronze, partition_key)
    """
    item_identifier = _extract_item_identifier(payload)
    line_ref        = _extract_first_line_ref(payload)
    partition_key   = get_partition_key(line_ref) if line_ref else "bus-tramway"

    dedup_key = f"{source}:{item_identifier}" if item_identifier \
                else f"{source}:{json.dumps(payload, sort_keys=True)}"

    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, dedup_key))

    envelope = {
        "event_id":            event_id,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source":              source,
        "stop_name":           stop_name,       # ← ajout : arrêt interrogé
        "partition_key":       partition_key,   # ← ajout : pour traçabilité
        "payload":             payload          # ← SIRI brut, jamais modifié
    }

    return envelope, partition_key


def _extract_item_identifier(payload: dict) -> str | None:
    try:
        visits = (
            payload["Siri"]["ServiceDelivery"]
                   ["StopMonitoringDelivery"][0]
                   ["MonitoredStopVisit"]
        )
        if visits:
            return visits[0].get("ItemIdentifier")
    except (KeyError, IndexError, TypeError):
        pass
    return None


def _extract_first_line_ref(payload: dict) -> str | None:
    """
    Extrait le premier line_ref du payload SIRI.
    Utilisé pour déterminer la partition key.
    """
    try:
        visits = (
            payload["Siri"]["ServiceDelivery"]
                   ["StopMonitoringDelivery"][0]
                   ["MonitoredStopVisit"]
        )
        if visits:
            return (
                visits[0]["MonitoredVehicleJourney"]
                         ["LineRef"]["value"]
            )
    except (KeyError, IndexError, TypeError):
        pass
    return None


# ============================================================
# TRANSIT PRODUCER
# ============================================================

class TransitProducer:

    def __init__(self):
        self.prim_client = PrimClient()
        self.producer    = self._build_producer()

    def _build_producer(self) -> KafkaProducer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(
                    v, ensure_ascii=False
                ).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                compression_type="lz4",
            )
            logger.info(f"✅ Connecté à Kafka — {config.KAFKA_BOOTSTRAP_SERVERS}")
            return producer

        except NoBrokersAvailable:
            logger.error(
                "❌ Impossible de se connecter à Kafka.\n"
                "   → Vérifie que Docker est lancé : docker-compose up -d\n"
                f"   → Broker attendu : {config.KAFKA_BOOTSTRAP_SERVERS}"
            )
            raise

    def _publish(self, topic: str, event: dict, partition_key: str) -> bool:
        try:
            future = self.producer.send(
                topic,
                key=partition_key,
                value=event
            )
            record_metadata = future.get(timeout=10)
            logger.info(
                f"📨 Publié → topic={record_metadata.topic} "
                f"partition={record_metadata.partition} "
                f"offset={record_metadata.offset} "
                f"key={partition_key}"
            )
            return True

        except KafkaError as e:
            logger.error(f"❌ Erreur Kafka lors de l'envoi : {e}")
            return False

    def run_passages(self) -> None:
        """
        Interroge TOUS les arrêts définis dans STOPS
        et publie chaque réponse dans la bonne partition.

        Chaque arrêt = 1 appel API = 1 event Kafka
        La partition est déterminée par le line_ref de la réponse
        """
        logger.info(f"📡 Poll passages — {len(STOPS)} arrêts à interroger")

        for stop in STOPS:
            raw = self.prim_client.get_passages(
                monitoring_ref=stop["ref"]
            )

            if not raw:
                logger.warning(f"⚠️ Pas de données pour {stop['name']} — skip")
                continue

            # Vérifier qu'il y a bien des passages dans la réponse
            try:
                visits = (
                    raw["Siri"]["ServiceDelivery"]
                       ["StopMonitoringDelivery"][0]
                       .get("MonitoredStopVisit", [])
                )
                if not visits:
                    logger.warning(f"⚠️ Aucun passage pour {stop['name']} — skip")
                    continue
            except (KeyError, IndexError):
                continue

            # Construction enveloppe + partition key
            event, partition_key = build_envelope(
                raw,
                source="prim_stop_monitoring",
                stop_name=stop["name"]
            )

            logger.info(
                f"   → {stop['name']} "
                f"| partition: {partition_key} "
                f"| passages: {len(visits)}"
            )

            self._publish(config.KAFKA_TOPIC_PASSAGES, event, partition_key)

    def run_alerts(self) -> None:
        raw = self.prim_client.get_alerts()
        if not raw:
            logger.warning("⚠️ Pas de données alertes — skip")
            return

        event, partition_key = build_envelope(
            raw,
            source="prim_general_message",
            stop_name="global"
        )
        self._publish(config.KAFKA_TOPIC_ALERTS, event, "alerts")

    def run(self) -> None:
        logger.info(f"🚀 Producer démarré — poll toutes les {config.POLL_INTERVAL_SECONDS}s")
        config.validate_config()

        cycle = 0

        try:
            while True:
                cycle += 1
                logger.info(f"--- Cycle #{cycle} ---")

                self.run_passages()

                # Alertes toutes les 5 minutes
                if cycle % 5 == 0:
                    self.run_alerts()

                self.producer.flush()
                logger.info(f"💤 Attente {config.POLL_INTERVAL_SECONDS}s...")
                time.sleep(config.POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("⛔ Arrêt manuel du producer (Ctrl+C)")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Producer arrêté proprement")


# ============================================================
# POINT D'ENTRÉE
# ============================================================
if __name__ == "__main__":
    producer = TransitProducer()
    producer.run()
