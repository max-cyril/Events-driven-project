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
# ENVELOPPE BRONZE
# ============================================================
# On ne modifie JAMAIS le payload — c'est la donnée brute SIRI.
# On ajoute uniquement les métadonnées pipeline qui sont
# IMPOSSIBLES à reconstituer plus tard :
#   - event_id    : identifiant unique déterministe (UUID v5)
#   - ingestion_timestamp : quand notre pipeline a reçu la donnée
#   - source      : quelle API a fourni la donnée

def build_envelope(payload: dict, source: str) -> dict:
    """
    Construit l'enveloppe Bronze autour du payload SIRI brut.

    L'event_id est un UUID v5 déterministe basé sur :
    - Le namespace URL (standard UUID)
    - L'ItemIdentifier PRIM si disponible, sinon le hash du payload entier

    Avantage : même payload = même UUID → déduplication triviale en S2
    avec df.dropDuplicates(["event_id"])

    Args:
        payload : JSON SIRI brut retourné par PrimClient (non modifié)
        source  : nom de l'API source (ex: "prim_stop_monitoring")

    Returns:
        dict : enveloppe Bronze prête à être sérialisée et envoyée à Kafka
    """

    # On cherche l'ItemIdentifier PRIM dans le payload
    # C'est l'ID unique de PRIM pour chaque passage — parfait pour UUID v5
    # Structure : payload["Siri"]["ServiceDelivery"]["StopMonitoringDelivery"][0]
    #                    ["MonitoredStopVisit"][0]["ItemIdentifier"]
    item_identifier = _extract_item_identifier(payload)

    if item_identifier:
        # UUID v5 basé sur l'ItemIdentifier PRIM
        # Même ItemIdentifier = même UUID → déduplication parfaite
        dedup_key = f"{source}:{item_identifier}"
    else:
        # Fallback : UUID v5 basé sur le contenu complet du payload
        # Moins précis mais toujours déterministe
        dedup_key = f"{source}:{json.dumps(payload, sort_keys=True)}"

    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, dedup_key))

    return {
        "event_id": event_id,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": payload          # ← SIRI brut, jamais modifié
    }


def _extract_item_identifier(payload: dict) -> str | None:
    """
    Extrait le premier ItemIdentifier trouvé dans le payload SIRI.
    Retourne None si absent ou structure inattendue.
    """
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


# ============================================================
# TRANSIT PRODUCER
# ============================================================

class TransitProducer:
    """
    Producer Kafka pour les données de transport en commun PRIM.

    Responsabilités :
    - Connexion au broker Kafka
    - Poll de l'API PRIM à intervalle régulier
    - Construction de l'enveloppe Bronze
    - Publication dans les topics Kafka
    - Gestion des erreurs et reconnexion
    """

    def __init__(self):
        self.prim_client = PrimClient()
        self.producer = self._build_producer()

    def _build_producer(self) -> KafkaProducer:
        """
        Construit le KafkaProducer avec sérialisation JSON automatique.

        value_serializer : convertit automatiquement chaque message
                           dict → bytes JSON UTF-8 avant envoi
                           → on n'a jamais besoin de sérialiser manuellement

        acks="all" : le broker confirme l'écriture sur TOUS les replicas
                     avant d'accuser réception
                     → garantie de durabilité maximale (important pour Bronze)

        retries : nombre de tentatives si l'envoi échoue
        """
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,

                # Sérialisation automatique dict → bytes JSON
                value_serializer=lambda v: json.dumps(
                    v, ensure_ascii=False
                ).encode("utf-8"),

                # Clé sérialisée en bytes (utilisée pour le partitionnement)
                key_serializer=lambda k: k.encode("utf-8") if k else None,

                # Durabilité maximale — on ne perd aucun message Bronze
                acks="all",

                # Retry sur erreurs réseau transitoires
                retries=3,

                # Compression des messages (réduit la bande passante)
                # lz4 = bon compromis vitesse/compression
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
        """
        Publie un event dans un topic Kafka.

        La partition_key détermine dans quelle partition le message va.
        On utilise le line_id comme clé → tous les events d'une même ligne
        vont dans la même partition → lecture ordonnée par ligne en S2.

        Args:
            topic         : nom du topic Kafka
            event         : enveloppe Bronze complète
            partition_key : clé de partitionnement (ex: "C01382")

        Returns:
            bool : True si succès, False si erreur
        """
        try:
            future = self.producer.send(
                topic,
                key=partition_key,
                value=event
            )

            # get() attend la confirmation du broker (mode synchrone)
            # timeout=10s — si pas de réponse en 10s, on considère en échec
            # En prod haute fréquence on utiliserait le mode async avec callbacks
            # mais pour S1 le mode synchrone est plus simple à debugger
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
        Poll l'API PRIM et publie les passages dans Kafka.
        Un appel = un event enveloppé (pas un event par passage).

        Pourquoi 1 event pour toute la réponse et pas 1 event par passage ?
        → On préserve la réponse API exactement telle quelle (Bronze pur)
        → Spark en S2 explose la liste des passages avec explode()
        → Plus simple, plus fiable, zéro perte de contexte
        """
        logger.info("📡 Poll passages PRIM...")
        raw = self.prim_client.get_passages()

        if not raw:
            logger.warning("⚠️ Pas de données passages — skip")
            return

        # Construction de l'enveloppe Bronze
        event = build_envelope(raw, source="prim_stop_monitoring")

        # Clé de partition = "passages" (toutes les réponses dans la même partition)
        self._publish(config.KAFKA_TOPIC_PASSAGES, event, partition_key="passages")

    def run_alerts(self) -> None:
        """
        Poll l'API PRIM et publie les alertes dans Kafka.
        """
        logger.info("🚨 Poll alertes PRIM...")
        raw = self.prim_client.get_alerts()

        if not raw:
            logger.warning("⚠️ Pas de données alertes — skip")
            return

        event = build_envelope(raw, source="prim_general_message")
        self._publish(config.KAFKA_TOPIC_ALERTS, event, partition_key="alerts")

    def run(self) -> None:
        """
        Boucle principale du producer.

        Toutes les POLL_INTERVAL_SECONDS :
        - Poll passages
        - Poll alertes (moins fréquent : toutes les 5 minutes)

        On compte les cycles pour réduire la fréquence des alertes
        sans avoir deux boucles séparées.
        """
        logger.info(f"🚀 Producer démarré — poll toutes les {config.POLL_INTERVAL_SECONDS}s")
        config.validate_config()

        cycle = 0

        try:
            while True:
                cycle += 1
                logger.info(f"--- Cycle #{cycle} ---")

                # Passages : chaque cycle
                self.run_passages()

                # Alertes : tous les 5 cycles (toutes les ~5 minutes)
                # Les perturbations changent moins souvent que les passages
                if cycle % 5 == 0:
                    self.run_alerts()

                # Flush : force l'envoi de tous les messages en attente
                # avant de dormir (important avec la compression lz4)
                self.producer.flush()

                logger.info(f"💤 Attente {config.POLL_INTERVAL_SECONDS}s...")
                time.sleep(config.POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("⛔ Arrêt manuel du producer (Ctrl+C)")
        finally:
            # On ferme proprement la connexion Kafka
            # flush() final pour ne perdre aucun message en attente
            self.producer.flush()
            self.producer.close()
            logger.info("✅ Producer arrêté proprement")


# ============================================================
# POINT D'ENTRÉE
# ============================================================
if __name__ == "__main__":
    producer = TransitProducer()
    producer.run()
