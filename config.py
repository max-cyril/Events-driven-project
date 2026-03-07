import os
from dotenv import load_dotenv

# ============================================================
# CHARGEMENT DU FICHIER .env
# ============================================================
# load_dotenv() lit le fichier .env à la racine du projet
# et injecte chaque variable dans os.environ
# Si la variable existe déjà dans l'environnement système,
# elle ne sera PAS écrasée (comportement par défaut)
load_dotenv()


def _get_required(key: str) -> str:
    """
    Récupère une variable d'environnement obligatoire.
    Lève une erreur explicite si elle est absente.
    On préfère crasher au démarrage plutôt qu'en plein milieu d'un run.
    """
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variable d'environnement manquante : '{key}'\n"
            f"Vérifie ton fichier .env (voir .env.example)"
        )
    return value


def _get_optional(key: str, default: str) -> str:
    """
    Récupère une variable d'environnement optionnelle.
    Retourne la valeur par défaut si absente.
    """
    return os.getenv(key, default)



PRIM_API_KEY: str = _get_required("PRIM_API_KEY")

# URL de base de l'API PRIM
PRIM_BASE_URL: str = _get_required("PRIM_BASE_URL")

# ============================================================
# KAFKA
# ============================================================
# BOOTSTRAP_SERVERS = adresse(s) du/des broker(s) Kafka
# C'est le point d'entrée — Kafka s'occupe du reste (découverte des partitions, etc.)
# Plusieurs brokers séparés par des virgules : "broker1:9092,broker2:9092"
KAFKA_BOOTSTRAP_SERVERS: str = _get_optional(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# ============================================================
# TOPICS
# ============================================================
# Convention de nommage : <domaine>.<tier>.<type>
# transit  → domaine métier
# raw      → données brutes, non transformées (comme la couche Bronze)
# passages → type d'event
KAFKA_TOPIC_PASSAGES: str = _get_optional(
    "KAFKA_TOPIC_PASSAGES", "transit.raw.passages"
)
KAFKA_TOPIC_ALERTS: str = _get_optional(
    "KAFKA_TOPIC_ALERTS", "transit.raw.alerts"
)

# ============================================================
# POLLING
# ============================================================
# Intervalle entre chaque appel à l'API PRIM (en secondes)
# 60s = safe par rapport au quota de 1000 req/jour
# 86400s / 60s = 1440 requêtes/jour → on reste sous le quota
POLL_INTERVAL_SECONDS: int = int(
    _get_optional("POLL_INTERVAL_SECONDS", "60")
)


# ============================================================
# VALIDATION AU DÉMARRAGE
# ============================================================
# Cette fonction est appelée une seule fois au lancement du producer
# Elle vérifie que tout est bien configuré avant de commencer à produire
def validate_config() -> None:
    """Valide la configuration au démarrage. Lève une erreur si invalide."""

    if POLL_INTERVAL_SECONDS < 30:
        raise ValueError(
            f"POLL_INTERVAL_SECONDS={POLL_INTERVAL_SECONDS} trop faible. "
            f"Minimum recommandé : 30s (respect du quota API PRIM)"
        )

    print("✅ Configuration valide")
    print(f"   KAFKA_BOOTSTRAP_SERVERS : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   TOPIC passages          : {KAFKA_TOPIC_PASSAGES}")
    print(f"   TOPIC alerts            : {KAFKA_TOPIC_ALERTS}")
    print(f"   Poll interval           : {POLL_INTERVAL_SECONDS}s")
    print(f"   PRIM_BASE_URL           : {PRIM_BASE_URL}")
    print(f"   PRIM_API_KEY            : ***{PRIM_API_KEY[-4:]}")
