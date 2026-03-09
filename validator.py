import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================
# STATISTIQUES EN TEMPS RÉEL
# ============================================================
# On suit quelques métriques simples pour valider que le pipeline
# fonctionne correctement — pas de stockage, juste des compteurs
# en mémoire pour la durée du validator

class PipelineStats:
    """
    Compteurs simples pour valider le pipeline en temps réel.
    Réinitialisés à chaque démarrage du validator.
    """
    def __init__(self):
        self.total_messages    = 0
        self.messages_by_partition = {}   # {partition: count}
        self.messages_by_key   = {}       # {partition_key: count}
        self.started_at        = datetime.now(timezone.utc)

    def record(self, partition: int, key: str) -> None:
        self.total_messages += 1
        self.messages_by_partition[partition] = (
            self.messages_by_partition.get(partition, 0) + 1
        )
        self.messages_by_key[key] = (
            self.messages_by_key.get(key, 0) + 1
        )

    def print_summary(self) -> None:
        elapsed = (datetime.now(timezone.utc) - self.started_at).seconds
        print("\n" + "=" * 50)
        print(f" 📊 Résumé pipeline — {elapsed}s de validation")
        print("=" * 50)
        print(f" Total messages reçus : {self.total_messages}")
        print(f"\n Messages par partition key :")
        for key, count in sorted(self.messages_by_key.items()):
            print(f"   {key:<15} → {count} messages")
        print(f"\n Messages par partition Kafka :")
        for partition, count in sorted(self.messages_by_partition.items()):
            print(f"   Partition {partition}     → {count} messages")
        print("=" * 50 + "\n")


# ============================================================
# VALIDATOR
# ============================================================

class PipelineValidator:
    """
    Consumer Kafka de validation.

    Responsabilités :
    - S'abonner aux topics transit.raw.*
    - Afficher chaque message reçu de façon lisible
    - Valider la structure de l'enveloppe Bronze
    - Afficher des statistiques en temps réel

    Ce n'est PAS un consumer de production — il ne stocke rien,
    ne transforme rien. Son seul rôle est de confirmer que le
    pipeline producer → Kafka fonctionne correctement.
    """

    def __init__(self):
        self.consumer = self._build_consumer()
        self.stats    = PipelineStats()

    def _build_consumer(self) -> KafkaConsumer:
        """
        Construit le KafkaConsumer.

        group_id : identifiant du groupe de consumers
                   Kafka retient la position (offset) de chaque groupe
                   → si tu redémarres le validator, il reprend où il s'était arrêté

        auto_offset_reset="earliest" : si le groupe n'a jamais consommé ce topic,
                                        commence depuis le début
                                        → on ne rate aucun message déjà dans Kafka

        enable_auto_commit=True : Kafka enregistre automatiquement l'offset
                                   après chaque message lu
                                   → comportement standard pour un consumer simple
        """
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_PASSAGES,
                config.KAFKA_TOPIC_ALERTS,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,

                # Désérialisation automatique bytes → dict JSON
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,

                # Groupe de consumers — identifiant unique pour le validator
                group_id="transit-validator",

                # Commence depuis le début si premier démarrage
                auto_offset_reset="earliest",

                # Commit automatique de l'offset
                enable_auto_commit=True,

                # Timeout si aucun message pendant 1s
                # → permet d'afficher le résumé périodiquement
                consumer_timeout_ms=1000,
            )
            logger.info(f"✅ Consumer connecté à Kafka")
            logger.info(f"   Topics : {config.KAFKA_TOPIC_PASSAGES}, {config.KAFKA_TOPIC_ALERTS}")
            return consumer

        except NoBrokersAvailable:
            logger.error(
                "❌ Impossible de se connecter à Kafka.\n"
                "   → Vérifie que Docker est lancé : docker-compose up -d\n"
                f"   → Broker attendu : {config.KAFKA_BOOTSTRAP_SERVERS}"
            )
            raise

    def _validate_envelope(self, event: dict) -> bool:
        """
        Valide que l'enveloppe Bronze contient tous les champs obligatoires.

        C'est la validation minimale — on vérifie la structure,
        pas le contenu du payload (qui est du SIRI brut).

        Returns:
            bool : True si valide, False sinon
        """
        required_fields = [
            "event_id",
            "ingestion_timestamp",
            "source",
            "payload",
            "partition_key",
        ]
        missing = [f for f in required_fields if f not in event]

        if missing:
            logger.warning(f"⚠️ Champs manquants dans l'enveloppe : {missing}")
            return False
        return True

    def _display_message(self, record) -> None:
        """
        Affiche un message Kafka de façon lisible dans le terminal.
        """
        event = record.value

        # Validation de l'enveloppe
        is_valid = self._validate_envelope(event)

        # Extraction des infos clés pour l'affichage
        event_id   = event.get("event_id", "?")[:8]   # 8 premiers chars de l'UUID
        source     = event.get("source", "?")
        stop_name  = event.get("stop_name", "?")
        part_key   = event.get("partition_key", "?")
        ingested   = event.get("ingestion_timestamp", "?")

        # Compte les passages dans le payload
        try:
            visits = (
                event["payload"]["Siri"]["ServiceDelivery"]
                     ["StopMonitoringDelivery"][0]
                     .get("MonitoredStopVisit", [])
            )
            nb_passages = len(visits)
        except (KeyError, IndexError, TypeError):
            nb_passages = 0

        # Affichage
        status = "✅" if is_valid else "❌"
        print(
            f"{status} "
            f"[{record.topic}] "
            f"partition={record.partition} "
            f"offset={record.offset} "
            f"| key={part_key:<12} "
            f"| stop={stop_name:<30} "
            f"| passages={nb_passages:<3} "
            f"| id={event_id}... "
            f"| ingested={ingested}"
        )

        # Mise à jour des stats
        self.stats.record(record.partition, part_key)

    def run(self) -> None:
        """
        Boucle principale du validator.

        Lit les messages en continu et affiche un résumé
        toutes les 30 secondes.
        """
        logger.info("🔍 Validator démarré — en attente de messages...")
        logger.info("   Appuie sur Ctrl+C pour arrêter et voir le résumé final\n")

        cycle = 0

        try:
            while True:
                cycle += 1
                messages_this_cycle = 0

                # poll() retourne les messages disponibles
                # consumer_timeout_ms=1000 → on attend max 1s
                # Si pas de messages → StopIteration → on continue la boucle
                try:
                    for record in self.consumer:
                        self._display_message(record)
                        messages_this_cycle += 1

                except StopIteration:
                    # Timeout — pas de messages pendant 1s
                    pass

                # Résumé toutes les 30 itérations (~30s)
                if cycle % 30 == 0 and self.stats.total_messages > 0:
                    self.stats.print_summary()

        except KeyboardInterrupt:
            logger.info("⛔ Arrêt du validator (Ctrl+C)")
        finally:
            self.stats.print_summary()
            self.consumer.close()
            logger.info("✅ Validator arrêté proprement")


# ============================================================
# POINT D'ENTRÉE
# ============================================================
if __name__ == "__main__":
    validator = PipelineValidator()
    validator.run()
