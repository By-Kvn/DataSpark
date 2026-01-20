# DataSpark - Pipeline ETL Moderne avec Architecture Medallion

## 📋 Présentation du Projet

**DataSpark** est un pipeline ETL (Extract, Transform, Load) moderne implémentant l'**architecture Medallion** (Bronze-Silver-Gold) pour le traitement de données Big Data. Ce projet démontre l'utilisation de technologies industrielles pour construire un système de traitement de données scalable, robuste et maintenable.

### 🎯 Objectifs du Projet

1. **Implémenter une architecture de Data Lake moderne** avec séparation claire des couches de données
2. **Automatiser le traitement de données** avec orchestration de workflows
3. **Assurer la qualité des données** à travers des validations et transformations
4. **Créer des données analytiques prêtes à l'emploi** pour la prise de décision

---

## 🏗️ Architecture du Système

### Architecture Medallion (Bronze-Silver-Gold)

Le projet suit l'architecture **Medallion** popularisée par Databricks, qui organise les données en trois couches distinctes :

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCES (CSV Files)                       │
│              clients.csv, achats.csv                         │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER (Raw Data Lake)                            │
│  • Données brutes, non transformées                         │
│  • Format: CSV                                               │
│  • Bucket MinIO: bronze/                                     │
│  • Objectif: Archive des données sources                    │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼ Transformation & Nettoyage
┌─────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER (Cleaned & Validated Data)                  │
│  • Données nettoyées et validées                            │
│  • Format: Parquet (optimisé pour l'analytique)             │
│  • Bucket MinIO: silver/                                     │
│  • Objectif: Données de qualité pour l'analyse             │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼ Agrégation & Enrichissement
┌─────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER (Analytical Data)                            │
│  • Tables de dimension (star schema)                        │
│  • Tables de faits                                           │
│  • KPIs et métriques                                         │
│  • Agrégations temporelles                                  │
│  • Bucket MinIO: gold/                                       │
│  • Objectif: Données prêtes pour la visualisation/BI        │
└─────────────────────────────────────────────────────────────┘
```

### Stack Technologique

| Composant | Technologie | Rôle |
|-----------|-------------|------|
| **Orchestration** | Prefect 3.x | Orchestration des workflows ETL avec gestion d'erreurs et retry automatique |
| **Stockage** | MinIO | Stockage objet S3-compatible pour le Data Lake |
| **Base de données** | PostgreSQL | Base de données pour Prefect (métadonnées des workflows) |
| **Traitement** | Apache Spark 3.5 | Traitement distribué des données (cluster master + 2 workers) |
| **Données** | Pandas + PyArrow | Manipulation et format Parquet |
| **Visualisation** | Streamlit | Interface de visualisation (prévu) |
| **Conteneurisation** | Docker Compose | Orchestration des services |

---

## 🔄 Fonctionnement du Pipeline

### Étape 1 : Bronze Layer (Ingestion)

**Objectif** : Ingérer les données brutes depuis les sources vers le Data Lake.

**Processus** :
1. **Upload vers Sources** : Les fichiers CSV (`clients.csv`, `achats.csv`) sont uploadés dans le bucket `sources` de MinIO
2. **Copie vers Bronze** : Les données sont copiées dans le bucket `bronze` sans transformation
3. **Format** : CSV (format source préservé)

**Fichiers traités** :
- `clients.csv` : Informations sur les clients (ID, nom, email, date d'inscription, pays)
- `achats.csv` : Transactions d'achats (ID achat, ID client, date, montant, produit)

**Résultat** : Données brutes archivées dans MinIO, prêtes pour la transformation.

---

### Étape 2 : Silver Layer (Transformation)

**Objectif** : Nettoyer, valider et transformer les données brutes en données de qualité.

**Transformations appliquées** :

1. **Nettoyage des valeurs nulles**
   - Détection et suppression des lignes avec valeurs nulles critiques
   - Statistiques de nettoyage générées

2. **Standardisation des dates**
   - Conversion des formats de dates vers un format standardisé
   - Validation de la cohérence temporelle

3. **Normalisation des types de données**
   - Conversion des types (int, float, datetime, string)
   - Optimisation de la mémoire

4. **Déduplication**
   - Suppression des enregistrements dupliqués
   - Conservation des données uniques

5. **Contrôles qualité**
   - Validation de l'intégrité référentielle
   - Vérification des contraintes métier
   - Génération de rapports de qualité

**Format de sortie** : Parquet (format colonnaire optimisé pour l'analytique)

**Résultats obtenus** :
- **Clients** : 1500 → 1479 lignes (21 lignes avec valeurs nulles supprimées)
- **Achats** : 23663 → 22422 lignes (1241 lignes avec valeurs nulles supprimées)
- Qualité validée : 0 valeurs nulles, 0 doublons, intégrité référentielle vérifiée

---

### Étape 3 : Gold Layer (Agrégation)

**Objectif** : Créer des données analytiques structurées pour la Business Intelligence.

**Composants créés** :

#### 1. Tables de Dimension (Star Schema)

- **`dim_client`** : Dimension clients enrichie (1479 clients)
- **`dim_produit`** : Dimension produits (10 produits uniques)
- **`dim_temps`** : Dimension temporelle avec attributs (jour, semaine, mois, trimestre, année)
- **`dim_pays`** : Dimension géographique (9 pays)

#### 2. Table de Faits

- **`fact_achats`** : Table de faits avec toutes les transactions (22422 lignes)
  - Clés étrangères vers les dimensions
  - Mesures : montant, quantité

#### 3. KPIs et Métriques

- **`kpi_volumes_jour`** : Volume de transactions par jour (346 jours)
- **`kpi_volumes_semaine`** : Volume de transactions par semaine (51 semaines)
- **`kpi_volumes_mois`** : Volume de transactions par mois (12 mois)
- **`kpi_ca_par_pays`** : Chiffre d'affaires par pays (9 pays)
- **`kpi_taux_croissance`** : Taux de croissance mensuel
- **`kpi_distributions_statistiques`** : Statistiques globales (moyenne, médiane, écart-type)

#### 4. Agrégations Temporelles

- **`agregation_jour`** : Agrégations journalières (CA, nombre de transactions, panier moyen)
- **`agregation_semaine`** : Agrégations hebdomadaires
- **`agregation_mois`** : Agrégations mensuelles

**Format de sortie** : Parquet (optimisé pour les requêtes analytiques)

---

## 🛠️ Ce qui a été Mis en Place

### 1. Infrastructure Docker

**Services déployés** :
- **MinIO** : Stockage objet (ports 9000/9001)
  - Buckets créés automatiquement : `sources`, `bronze`, `silver`, `gold`
  - Console d'administration accessible
  
- **PostgreSQL** : Base de données pour Prefect (port 5432)
  - Stocke les métadonnées des workflows
  - Historique des exécutions

- **Prefect Server** : Serveur d'orchestration (port 4200)
  - Interface web pour monitorer les workflows
  - Gestion des tâches et retry automatique

- **Apache Spark Cluster** :
  - Master (port 8080) : Interface de monitoring Spark
  - Worker 1 (port 8081) : Nœud de traitement
  - Worker 2 (port 8082) : Nœud de traitement

### 2. Code Python Structuré

**Organisation modulaire** :
```
flows/
├── config.py              # Configuration centralisée (MinIO, Prefect)
├── bronze_ingestion.py    # Flow Bronze : ingestion des données
├── silver_transformation.py  # Flow Silver : transformation et nettoyage
├── gold_aggregation.py    # Flow Gold : agrégations et KPIs
└── orchestration.py       # Orchestration complète du pipeline
```

**Caractéristiques** :
- **Prefect Tasks** : Chaque étape est une tâche Prefect avec retry automatique
- **Prefect Flows** : Orchestration des tâches avec gestion des dépendances
- **Gestion d'erreurs** : Retry automatique en cas d'échec
- **Logging** : Traçabilité complète des opérations

### 3. Scripts d'Automatisation

- **`setup.sh`** : Installation automatique de l'environnement Python
- **`start-services.sh`** : Démarrage des services Docker
- **`run.sh`** : Exécution du pipeline complet

### 4. Qualité et Robustesse

- **Validation des données** : Contrôles qualité à chaque étape
- **Gestion des erreurs** : Retry automatique sur les opérations critiques
- **Traçabilité** : Logs détaillés de chaque opération
- **Reproductibilité** : Pipeline idempotent (peut être exécuté plusieurs fois)

---

## 📊 Résultats Obtenus

### Données Traitées

| Couche | Fichiers | Lignes | Format |
|--------|----------|--------|--------|
| **Bronze** | 2 CSV | ~25,000 | CSV |
| **Silver** | 2 Parquet | 23,901 | Parquet |
| **Gold** | 15 Parquet | ~25,000+ | Parquet |

### Métriques de Qualité

- ✅ **0 valeurs nulles** dans les données Silver et Gold
- ✅ **0 doublons** détectés
- ✅ **Intégrité référentielle** validée
- ✅ **1479 clients uniques** traités
- ✅ **22422 transactions** analysées
- ✅ **10 produits** catalogués
- ✅ **9 pays** représentés

### KPIs Calculés

- Volume de transactions par période (jour/semaine/mois)
- Chiffre d'affaires par pays
- Taux de croissance mensuel
- Statistiques de distribution (moyenne, médiane, écart-type)
- Agrégations temporelles pour l'analyse de tendances

---

## 🚀 Utilisation

### Prérequis

- Docker Desktop installé et démarré
- Python 3.10+ installé
- Git (pour cloner le projet)

### Installation

```bash
# 1. Cloner le projet
git clone https://github.com/By-Kvn/DataSpark.git
cd DataSpark

# 2. Démarrer les services Docker
docker-compose up -d

# 3. Installer les dépendances Python
chmod +x setup.sh
./setup.sh

# 4. (Optionnel) Configurer Java pour PySpark
source setup_java.sh

# 5. (Optionnel) Générer des données de test
python script/generate_data.py
```

### Exécution du Pipeline

```bash
# Activer l'environnement virtuel
source venv/bin/activate

# Exécuter le pipeline complet
cd flows
python orchestration.py
```

Ou utiliser le script automatique :
```bash
chmod +x run.sh
./run.sh
```

### Accès aux Interfaces

- **Prefect UI** : http://localhost:4200
  - Visualiser les workflows
  - Consulter l'historique des exécutions
  - Monitorer les performances

- **MinIO Console** : http://localhost:9001
  - Login : `minioadmin` / `minioadmin`
  - Explorer les buckets et fichiers
  - Télécharger les données traitées

- **Spark Master UI** : http://localhost:8080
  - Monitorer le cluster Spark
  - Voir les applications en cours

---

## 📁 Structure du Projet

```
DataSpark/
├── flows/                              # Code source des flows Prefect
│   ├── config.py                      # Configuration centralisée (MinIO, Prefect, Spark)
│   ├── bronze_ingestion.py            # Couche Bronze
│   ├── silver_transformation.py       # Couche Silver (Pandas)
│   ├── silver_transformation_spark.py # Couche Silver (PySpark)
│   ├── gold_aggregation.py            # Couche Gold
│   └── orchestration.py               # Orchestration complète
├── script/
│   ├── generate_data.py               # Génération de données de test
│   └── benchmark_pandas_vs_spark.py   # Script de benchmark
├── data/
│   └── sources/                       # Données sources CSV
├── docker-compose.yml                 # Configuration Docker (MinIO, Prefect, Spark)
├── requirements.txt                   # Dépendances Python
├── setup.sh                           # Script d'installation
├── start-services.sh                  # Démarrage des services
├── run.sh                             # Exécution du pipeline
├── README.md                          # Ce fichier
├── README_USAGE.md                    # Guide d'utilisation détaillé
├── INSTALL.md                         # Guide d'installation
└── TROUBLESHOOTING.md                 # Guide de dépannage
```

---

## 🎓 Points Pédagogiques

Ce projet démontre :

1. **Architecture de Data Lake moderne** : Implémentation de l'architecture Medallion
2. **Orchestration de workflows** : Utilisation de Prefect pour automatiser les processus ETL
3. **Stockage objet** : Utilisation de MinIO (S3-compatible) pour le Data Lake
4. **Qualité des données** : Validation et nettoyage systématique
5. **Modélisation analytique** : Création d'un schéma en étoile (star schema)
6. **Calcul de KPIs** : Agrégations et métriques métier
7. **Conteneurisation** : Déploiement avec Docker Compose
8. **Bonnes pratiques** : Code modulaire, gestion d'erreurs, logging

---

## 🔮 Améliorations Futures

- [x] Intégration Spark pour le traitement distribué de grandes volumétries ✅
- [x] Script de benchmark Pandas vs PySpark ✅
- [ ] Version PySpark complète pour la couche Gold
- [ ] Interface Streamlit pour visualiser les KPIs
- [ ] Planification automatique (scheduling) avec Prefect
- [ ] Alertes et notifications en cas d'erreur
- [ ] Tests unitaires et d'intégration
- [ ] Documentation API avec Swagger
- [ ] Pipeline CI/CD avec GitHub Actions

---

## 📚 Technologies Utilisées

- **Python 3.10+** : Langage de programmation
- **Prefect 3.x** : Orchestration de workflows
- **MinIO** : Stockage objet S3-compatible
- **PostgreSQL** : Base de données relationnelle
- **Apache Spark 3.5** : Traitement distribué
- **Pandas** : Manipulation de données
- **PyArrow** : Format Parquet
- **Docker & Docker Compose** : Conteneurisation
- **Streamlit** : Interface de visualisation (prévu)

---

## 👤 Auteur

**Kevin Labatte**
- Projet éducatif réalisé dans le cadre d'un cours sur le Big Data
- Démonstration d'un pipeline ETL moderne avec architecture Medallion

---

## 📝 License

Ce projet est un exemple éducatif de pipeline ETL moderne. Libre d'utilisation pour l'apprentissage.

---

## 🙏 Remerciements

Ce projet utilise des technologies open-source et suit les meilleures pratiques de l'industrie pour le traitement de données Big Data.
