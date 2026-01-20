#!/bin/bash

# Script pour lancer le dashboard Streamlit
echo "🚀 Démarrage du dashboard Streamlit..."

# Activer l'environnement virtuel
if [ ! -d "venv" ]; then
    echo "❌ Environnement virtuel non trouvé. Exécutez d'abord ./setup.sh"
    exit 1
fi

source venv/bin/activate

# Vérifier que Streamlit est installé
if ! python -c "import streamlit" 2>/dev/null; then
    echo "📦 Installation de Streamlit..."
    pip install streamlit plotly
fi

# Lancer Streamlit
echo "🌐 Ouverture du dashboard sur http://localhost:8501"
streamlit run streamlit_app.py
