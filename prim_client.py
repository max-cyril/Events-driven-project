import logging
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)



class PrimClient:
    """
    Client HTTP pour l'API PRIM Île-de-France Mobilités.

    Responsabilités :
    - Authentification via header apikey
    - Retry automatique sur erreurs réseau
    - Timeout sur chaque requête
    - Retourne le JSON brut (pas de transformation ici)
    """

    def __init__(self):
        self.session = self._build_session()
        self.base_url = config.PRIM_BASE_URL

    def _build_session(self) -> requests.Session:
        """
        Construit une session HTTP avec :
        - Header apikey injecté sur toutes les requêtes
        - Retry automatique sur erreurs réseau et 5xx
        """
        session = requests.Session()

        # Header apikey — injecté automatiquement sur chaque requête
        session.headers.update({
            "apikey": config.PRIM_API_KEY,
            "Accept": "application/json"
        })

        # --------------------------------------------------------
        # RETRY STRATEGY
        # Retry automatique si la requête échoue à cause de :
        # - Problème réseau (ConnectionError, Timeout)
        # - Erreur serveur 5xx (le serveur PRIM est down)
        #
        # On NE retry PAS sur les 4xx (401, 403, 404)
        # car ce sont des erreurs côté client (mauvaise clé, mauvaise URL)
        # --------------------------------------------------------
        retry_strategy = Retry(
            total=3,                        # 3 tentatives maximum
            backoff_factor=2,               # attente : 2s, 4s, 8s entre chaque retry
            status_forcelist=[500, 502, 503, 504],  # codes HTTP qui déclenchent un retry
            allowed_methods=["GET"]         # on ne retry que les GET (idempotents)
        )

        # On attache la stratégie de retry à l'adaptateur HTTP
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        return session

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """
        Effectue un GET sur l'API PRIM.

        Args:
            endpoint : chemin relatif (ex: "/stop-monitoring")
            params   : paramètres de la requête (ex: MonitoringRef, LineRef)

        Returns:
            dict : JSON parsé si succès
            None : si erreur (déjà loggée)
        """
        url = f"{self.base_url}{endpoint}"

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=10  # 10s max — au-delà on considère que l'API ne répond pas
            )

            # raise_for_status() lève une exception si le code HTTP est >= 400
            # Ça évite de traiter silencieusement une réponse d'erreur
            response.raise_for_status()

            logger.info(f"✅ GET {endpoint} → {response.status_code}")
            return response.json()

        except requests.exceptions.Timeout:
            logger.error(f"⏱️ Timeout sur {url} après 10s")
            return None

        except requests.exceptions.ConnectionError:
            logger.error(f"🔌 Erreur de connexion sur {url}")
            return None

        except requests.exceptions.HTTPError as e:
            # On distingue les erreurs selon le code HTTP
            status = e.response.status_code if e.response else "?"

            if status == 401:
                logger.error("Clé API PRIM invalide ou expirée (401)")
            elif status == 403:
                logger.error("Accès refusé — vérifie tes droits sur cette API (403)")
            elif status == 429:
                logger.warning("Quota API dépassé — attente 60s (429)")
                time.sleep(60)
            else:
                logger.error(f"Erreur HTTP {status} sur {url}")

            return None

        except Exception as e:
            logger.error(f"Erreur inattendue : {e}")
            return None

    # ============================================================
    # MÉTHODES PUBLIQUES
    # ============================================================

    def get_passages(
        self,
        monitoring_ref: str =  "STIF:StopArea:SP:22046:",#"STIF:StopPoint:Q:41119:",
        line_ref: Optional[str] = None
    ) -> Optional[dict]:
        """
        Récupère les prochains passages à un arrêt donné.

        Args:
            monitoring_ref : identifiant de l'arrêt PRIM
                             défaut = Châtelet-Les-Halles
            line_ref       : filtre optionnel par ligne
                             ex: "STIF:Line::C01371:" = RER A

        Returns:
            dict : JSON SIRI brut des prochains passages
        """
        params = {"MonitoringRef": monitoring_ref}

        if line_ref:
            params["LineRef"] = line_ref

        logger.info(f" Récupération passages — arrêt: {monitoring_ref}")
        return self._get("/stop-monitoring", params=params)

    def get_alerts(self, line_id: str = "C01371") -> Optional[dict]:
        """
        Récupère les perturbations en cours sur une ligne.

        Args:
            line_id : identifiant court de la ligne
                      ex: "C01371" = RER A

        Returns:
            dict : JSON des perturbations actives
        """
        # endpoint = f"/navitia/coverage/fr-idf/lines/line:IDFM:{line_id}/disruptions"
        endpoint = f"/general-message?LineRef=STIF:Line::{line_id}:"
        logger.info(f"🚨 Récupération alertes — ligne: {line_id}")
        return self._get(endpoint)


# ============================================================
# TEST RAPIDE (si lancé directement)
# ============================================================
if __name__ == "__main__":
    client = PrimClient()

    print("\n--- TEST 1 : Prochains passages ---")
    passages = client.get_passages()
    if passages:
        # On navigue dans le SIRI pour compter les passages reçus
        try:
            visits = (
                passages["Siri"]["ServiceDelivery"]
                        ["StopMonitoringDelivery"][0]
                        ["MonitoredStopVisit"]
            )
            print(f"✅ {len(visits)} passages reçus")
        except (KeyError, IndexError) as e:
            print(f"⚠️ Structure SIRI inattendue : {e}")

    print("\n--- TEST 2 : Alertes RER A ---")
    alerts = client.get_alerts("22046") # ID de la ligne RER A
    if alerts:
        disruptions = alerts.get("disruptions", [])
        print(f"✅ {len(disruptions)} perturbations actives sur le RER A")


