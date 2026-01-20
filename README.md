# DataSpark

Pipeline ETL moderne utilisant Prefect, MinIO, Spark et Streamlit pour le traitement de données Big Data.

## 🎯 Vue d'ensemble

DataSpark est un pipeline ETL (Extract, Transform, Load) organisé en couches **Bronze**, **Silver** et **Gold** :

- **Bronze** : Ingestion brute des données depuis les sources
- **Silver** : Transformation et nettoyage des données
- **Gold** : Agrégations et données analytiques prêtes à l'emploi

## 🏗️ Architecture

- **Prefect** : Orchestration des workflows ETL
- **MinIO** : Stockage objet (S3-compatible) pour les données
- **Apache Spark** : Traitement distribué des données
- **PostgreSQL** : Base de données pour Prefect
- **Streamlit** : Interface de visualisation

## 🚀 Démarrage rapide

Voir le [Guide d'utilisation complet](README_USAGE.md) pour les instructions détaillées.

### Installation

```bash
# 1. Démarrer Docker Desktop (macOS)
# 2. Démarrer les services
docker-compose up -d

# 3. Installer les dépendances Python
chmod +x setup.sh
./setup.sh

# 4. Générer des données de test (optionnel)
python script/generate_data.py

# 5. Exécuter le pipeline
python flows/orchestration.py
```

## 📁 Structure du projet

```
BigData-ex/
├── flows/              # Flows Prefect (Bronze, Silver, Gold)
├── script/             # Scripts utilitaires
├── data/               # Données sources et générées
├── docker-compose.yml  # Configuration Docker
└── requirements.txt    # Dépendances Python
```

## 📚 Documentation

- [Guide d'utilisation](README_USAGE.md) - Instructions complètes
- [Installation](INSTALL.md) - Guide d'installation détaillé
- [Dépannage](TROUBLESHOOTING.md) - Solutions aux problèmes courants

## 🛠️ Technologies

- Python 3.10+
- Prefect 3.x
- Apache Spark 3.5
- MinIO
- PostgreSQL
- Streamlit
- Pandas, PyArrow

## 📝 License

Ce projet est un exemple éducatif de pipeline ETL moderne.
