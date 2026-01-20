#!/bin/bash

# Script pour exécuter le pipeline avec l'environnement virtuel
echo "🚀 Démarrage du pipeline ETL..."

# Activer l'environnement virtuel
if [ ! -d "venv" ]; then
    echo "❌ Environnement virtuel non trouvé. Exécutez d'abord ./setup.sh"
    exit 1
fi

source venv/bin/activate

# Aller dans le répertoire flows
cd flows

# Exécuter le pipeline
echo "▶️  Exécution du pipeline..."
python3 orchestration.py
