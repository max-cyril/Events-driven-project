# Events-Driven-Project — Semaine 1

Pipeline de données temps réel sur le réseau de transport francilien (PRIM IDFM).  
Ingestion des passages et perturbations via l'API PRIM → stockage dans un cluster Kafka 3 brokers.

---

## Ce qui a été construit en S1

```
PRIM API (IDFM)
      ↓
  prim_client.py       → appels HTTP avec retry/timeout
      ↓
  producer.py          → polling 60s + enveloppe Bronze + partition key
      ↓
  Kafka Cluster        → 3 brokers, 2 topics, 7 partitions, replication=3
      ↓
  validator.py         → consumer de validation bout-en-bout
```

---

## Stack

| Composant | Technologie |
|---|---|
| Message broker | Apache Kafka 7.5.0 (Confluent) |
| Coordination | Zookeeper 7.5.0 |
| UI monitoring | Kafdrop |
| Producer | Python 3.11+ |
| Source données | API PRIM IDFM (SIRI Lite) |
| Conteneurisation | Docker + Compose |

---

## Structure du projet

```
events-driven-project/
│
├── docker-compose.yml      ← Kafka cluster (3 brokers) + Zookeeper + Kafdrop
├── init-topics.sh          ← création explicite des topics (prod)
├── Makefile                ← orchestration des commandes
│
├── producer.py             ← boucle principale + partition key
├── prim_client.py          ← client HTTP PRIM (retry, timeout, session)
├── config.py               ← configuration centralisée depuis .env
├── validator.py            ← consumer de validation
│
├── .env                    ← secrets locaux (jamais commités)
├── .env.example            ← template
├── .gitignore
└── requirements.txt
```

---

## Prérequis

- Docker + Docker Compose
- Python 3.11+
- Clé API PRIM IDFM → https://prim.iledefrance-mobilites.fr

---

## Installation et démarrage

### 1. Cloner le projet

```bash
git clone https://github.com/<ton-compte>/events-driven-project.git
cd events-driven-project
```

### 2. Configurer les secrets

```bash
cp .env.example .env
# Éditer .env et renseigner PRIM_API_KEY
```

### 3. Lancer le cluster Kafka

```bash
docker-compose up -d

# Vérifier que les 3 brokers sont up
docker-compose ps

# UI Kafka disponible sur
http://localhost:9000
```

### 4. Créer les topics

```bash
chmod +x init-topics.sh
./init-topics.sh
```

### 5. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

### 6. Lancer le producer

```bash
python producer.py
```

### 7. Valider le pipeline (dans un second terminal)

```bash
python validator.py
```

---

## Architecture Kafka

### Cluster

| Broker | Port interne | Port externe |
|---|---|---|
| kafka-1 | 29092 | 9092 |
| kafka-2 | 29093 | 9093 |
| kafka-3 | 29094 | 9094 |

Configuration prod :
- `replication-factor = 3` → chaque message copié sur 3 brokers
- `min.insync.replicas = 2` → 2 brokers doivent confirmer l'écriture
- `acks = all` → durabilité maximale côté producer
- `auto.create.topics = false` → topics créés explicitement via `init-topics.sh`

### Topics

| Topic | Partitions | Replication | Rétention | Source |
|---|---|---|---|---|
| `transit.raw.passages` | 7 | 3 | 7 jours | API stop-monitoring PRIM |
| `transit.raw.alerts` | 1 | 3 | 7 jours | API general-message PRIM |

### Partitionnement de `transit.raw.passages`

| Partition | Clé | Lignes | Line ID PRIM |
|---|---|---|---|
| 0 | `rer-a` | RER A | `STIF:Line::C01371:` |
| 1 | `rer-b` | RER B | `STIF:Line::C01372:` |
| 2 | `rer-c` | RER C | `STIF:Line::C01373:` |
| 3 | `rer-d` | RER D | `STIF:Line::C01616:` |
| 4 | `rer-e` | RER E | `STIF:Line::C01374:` |
| 5 | `metro` | Toutes lignes métro | — |
| 6 | `bus-tramway` | Bus + Tramway | — |

Tous les events d'une même ligne vont dans la même partition → ordre chronologique garanti → calcul de retard fiable en S2.

---

## Format des messages Kafka

Chaque message publié dans Kafka respecte l'enveloppe **Bronze layer** :

```json
{
  "event_id": "uuid-v5-deterministe",
  "ingestion_timestamp": "2026-03-08T17:00:00Z",
  "source": "prim_stop_monitoring",
  "stop_name": "Saint-Michel (RER A/B/C)",
  "partition_key": "rer-a",
  "payload": {
    "Siri": {
      "ServiceDelivery": {
        "StopMonitoringDelivery": [
          {
            "MonitoredStopVisit": [ ... ]
          }
        ]
      }
    }
  }
}
```

### Principes Bronze layer

- `payload` = JSON SIRI brut **jamais modifié**
- `event_id` = UUID v5 déterministe basé sur l'`ItemIdentifier` PRIM → déduplication triviale en S2 via `df.dropDuplicates(["event_id"])`
- `ingestion_timestamp` = seule métadonnée impossible à reconstituer plus tard
- Toute transformation se fait en aval (Spark S2), jamais à l'ingestion

---

## Arrêts interrogés

Le producer interroge ces arrêts à chaque cycle (toutes les 60s) :

| Arrêt | Ref PRIM | Lignes couvertes |
|---|---|---|
| Saint-Michel | `STIF:StopArea:SP:22104:` | RER A, B, C |
| Auber | `STIF:StopArea:SP:22123:` | RER A |
| Nation | `STIF:StopArea:SP:22034:` | RER C |
| Villiers-le-Bel | `STIF:StopArea:SP:60640:` | RER D |
| Haussmann | `STIF:StopArea:SP:22144:` | RER E |
| Trinité | `STIF:StopArea:SP:22046:` | Métro 12 |
| Opéra | `STIF:StopArea:SP:22109:` | Bus |

---

## Variables d'environnement

| Variable | Obligatoire | Défaut | Description |
|---|---|---|---|
| `PRIM_API_KEY` | ✅ | — | Clé API PRIM IDFM |
| `KAFKA_BOOTSTRAP_SERVERS` | — | `localhost:9092,localhost:9093,localhost:9094` | Adresses des brokers |
| `KAFKA_TOPIC_PASSAGES` | — | `transit.raw.passages` | Topic passages |
| `KAFKA_TOPIC_ALERTS` | — | `transit.raw.alerts` | Topic alertes |
| `POLL_INTERVAL_SECONDS` | — | `60` | Intervalle de polling (min: 30s) |

---

## Décisions techniques

### Pourquoi 1 event par réponse API et pas 1 event par passage ?

On préserve la réponse SIRI complète dans un seul event Kafka (Bronze pur). Spark en S2 éclate les passages individuels avec `explode()`. Découper à l'ingestion ferait perdre le contexte de la réponse originale.

### Pourquoi UUID v5 déterministe ?

L'API PRIM poll toutes les 60s — le même passage apparaît dans plusieurs réponses consécutives. UUID v5 basé sur l'`ItemIdentifier` PRIM garantit que le même passage = le même UUID → `dropDuplicates("event_id")` en S2 sans logique complexe.

### Pourquoi 3 brokers en local ?

Approche prod dès le départ. `replication-factor=3` + `min.insync.replicas=2` → tolérance à la perte d'1 broker. Les volumes persistants Docker garantissent la durabilité entre restarts.

### Pourquoi `auto.create.topics=false` ?

On ne laisse jamais Kafka créer des topics silencieusement avec des paramètres par défaut. `init-topics.sh` contrôle explicitement partitions, replication et rétention.

---

## Roadmap

```
✅ S1  Kafka — ingestion Bronze (actuel)
⬜ S2  Spark Structured Streaming — enrichissement Silver → Postgres
⬜ S3  Metabase — dashboards ponctualité, retards, heatmap
⬜ S4  Kubernetes — orchestration complète de la plateforme
```

---

## Auteur

Projet personnel — Data Engineering portfolio  
Stack : Kafka · Spark · Postgres · Metabase · Docker · Python
