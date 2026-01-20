# Installation des dépendances

## Commandes à exécuter dans ton terminal

Copie-colle ces commandes une par une dans ton terminal :

```bash
# 1. Aller dans le répertoire du projet
cd /Users/kevin/Desktop/BigData-ex

# 2. Mettre à jour pip
python3 -m pip install --upgrade pip

# 3. Installer toutes les dépendances
python3 -m pip install prefect minio pandas pyarrow faker streamlit plotly python-dotenv

# 4. Vérifier l'installation
python3 -c "import prefect; print('✅ Prefect installé')"
python3 -c "import minio; print('✅ MinIO installé')"
python3 -c "import pandas; print('✅ Pandas installé')"
```

## Alternative : Installation depuis requirements.txt

```bash
cd /Users/kevin/Desktop/BigData-ex
python3 -m pip install -r requirements.txt
```

## Après l'installation

Une fois les dépendances installées, tu peux exécuter le pipeline :

```bash
cd flows
python3 orchestration.py
```
