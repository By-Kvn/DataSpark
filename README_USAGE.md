# Guide d'utilisation du Pipeline ETL

## 📋 Prérequis

1. **Docker et Docker Compose** installés
2. **Python 3.10+** avec les dépendances installées
3. **MinIO** et **Prefect** démarrés

## 🚀 Démarrage rapide

### 0. Démarrer Docker Desktop (macOS)

**⚠️ IMPORTANT :** Docker Desktop doit être démarré avant tout !

1. **Ouvrir Docker Desktop**
   - Appuyer sur `Cmd + Espace` (Spotlight)
   - Taper "Docker" et appuyer sur Entrée
   - OU aller dans Applications → Docker

2. **Attendre que Docker soit prêt**
   - L'icône Docker apparaît dans la barre de menu (en haut à droite)
   - Attendre qu'elle soit **verte** (pas orange/rouge)
   - Cela peut prendre 30 secondes à 2 minutes

3. **Vérifier que Docker fonctionne**
   ```bash
   docker ps
   ```
   Cette commande doit retourner une liste (même vide) **sans erreur**.

   Si tu as toujours l'erreur "Cannot connect to the Docker daemon", voir le fichier `TROUBLESHOOTING.md`

### 1. Démarrer les services (MinIO + Prefect)

```bash
docker-compose up -d
```

Cela démarre :
- MinIO sur `http://localhost:9000` (console: `http://localhost:9001`)
- Prefect Server sur `http://localhost:4200`
- PostgreSQL pour Prefect

### 2. Installer les dépendances Python

**⚠️ Important :** Sur macOS avec Homebrew, il faut utiliser un environnement virtuel.

**Option A : Script automatique (recommandé)**
```bash
chmod +x setup.sh
./setup.sh
```

**Option B : Installation manuelle avec venv**
```bash
# Créer l'environnement virtuel
python3 -m venv venv

# Activer l'environnement virtuel
source venv/bin/activate

# Installer les dépendances
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

**Option C : Installation avec --user (non recommandé)**
```bash
python3 -m pip install --user -r requirements.txt
```

### 3. Configurer l'environnement (optionnel)

Créer un fichier `.env` si besoin :

```env
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=False
PREFECT_API_URL=http://localhost:4200/api
```

## 🎯 Exécution du pipeline

### Option 1 : Pipeline complet avec script (recommandé)

```bash
# Activer l'environnement virtuel
source venv/bin/activate

# Exécuter le script
chmod +x run.sh
./run.sh
```

### Option 1bis : Pipeline complet manuel

```bash
# Activer l'environnement virtuel
source venv/bin/activate

# Exécuter le pipeline
cd flows
python3 orchestration.py
```

### Option 2 : Exécution étape par étape

#### Étape 1 : Bronze (Ingestion)
```bash
source venv/bin/activate
cd flows
python3 bronze_ingestion.py
```

#### Étape 2 : Silver (Transformation)
```bash
source venv/bin/activate
cd flows
python3 silver_transformation.py
```

#### Étape 3 : Gold (Agrégation)
```bash
source venv/bin/activate
cd flows
python3 gold_aggregation.py
```

### Option 3 : Via Prefect UI

1. Démarrer Prefect UI : `prefect server start` (ou utiliser Docker)
2. Accéder à `http://localhost:4200`
3. Créer un déploiement et exécuter les flows depuis l'interface

## 📊 Structure des données

### Bronze Layer
- **Format** : CSV (copie brute)
- **Bucket MinIO** : `bronze`
- **Fichiers** : `clients.csv`, `achats.csv`

### Silver Layer
- **Format** : Parquet (données nettoyées)
- **Bucket MinIO** : `silver`
- **Fichiers** : `clients.parquet`, `achats.parquet`
- **Transformations** :
  - ✅ Nettoyage des valeurs nulles
  - ✅ Suppression des valeurs aberrantes
  - ✅ Standardisation des dates (ISO)
  - ✅ Normalisation des types
  - ✅ Déduplication

### Gold Layer
- **Format** : Parquet (agrégations métier)
- **Bucket MinIO** : `gold`
- **Tables de dimensions** :
  - `dim_client.parquet`
  - `dim_produit.parquet`
  - `dim_temps.parquet`
  - `dim_pays.parquet`
- **Table de faits** :
  - `fact_achats.parquet`
- **KPIs** :
  - `kpi_volumes_jour.parquet`
  - `kpi_volumes_semaine.parquet`
  - `kpi_volumes_mois.parquet`
  - `kpi_ca_par_pays.parquet`
  - `kpi_taux_croissance.parquet`
  - `kpi_distributions_statistiques.parquet`
- **Agrégations temporelles** :
  - `agregation_jour.parquet`
  - `agregation_semaine.parquet`
  - `agregation_mois.parquet`

## 🔍 Vérification des résultats

### Via MinIO Console
1. Ouvrir `http://localhost:9001`
2. Se connecter avec `minioadmin` / `minioadmin`
3. Naviguer dans les buckets `bronze`, `silver`, `gold`

### Via Python
```python
from minio import Minio
from io import BytesIO
import pandas as pd

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Télécharger un fichier Gold
response = client.get_object("gold", "kpi_ca_par_pays.parquet")
df = pd.read_parquet(BytesIO(response.read()))
print(df.head())
```

## 🐛 Dépannage

### Erreur "Cannot connect to the Docker daemon"
- **Docker Desktop n'est pas démarré !**
- Sur macOS : Ouvrir Docker Desktop depuis Applications
- Attendre que l'icône Docker soit verte dans la barre de menu
- Vérifier : `docker ps` doit fonctionner

### Erreur "Connection refused" sur le port 9000
- **MinIO n'est pas démarré !** Exécuter : `docker-compose up -d`
- Vérifier que Docker est démarré : `docker ps`
- Vérifier que MinIO est actif : `docker ps | grep minio`
- Attendre quelques secondes après le démarrage pour que MinIO soit prêt
- Vérifier les variables d'environnement dans `.env`

### Erreur Prefect
- Vérifier que Prefect Server est démarré : `http://localhost:4200/api/health`
- Configurer l'URL : `export PREFECT_API_URL=http://localhost:4200/api`

### Erreur de dépendances
- Réinstaller : `pip3 install -r requirements.txt --upgrade`

### Erreur "command not found: python"
- Sur macOS/Linux, utiliser `python3` au lieu de `python`
- Vérifier l'installation : `python3 --version`

### Erreur "ModuleNotFoundError: No module named 'prefect'"
- Installer les dépendances : `./setup.sh` ou créer un venv manuellement
- **N'oublie pas d'activer l'environnement virtuel** : `source venv/bin/activate`
- Vérifier l'installation : `python3 -c "import prefect; print('OK')"`

### Erreur "externally-managed-environment"
- **Solution :** Utiliser un environnement virtuel (venv)
- Créer le venv : `python3 -m venv venv`
- Activer : `source venv/bin/activate`
- Puis installer : `python3 -m pip install -r requirements.txt`

## 📝 Notes

- Les données sont stockées dans MinIO (object storage)
- Le format Parquet est utilisé pour Silver et Gold (optimisé pour l'analytique)
- Les flows Prefect sont idempotents (peuvent être réexécutés sans problème)
