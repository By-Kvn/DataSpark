#!/bin/bash

# Script d'installation pour le pipeline ETL avec environnement virtuel
echo "🚀 Configuration de l'environnement Python..."

# Vérifier que python3 est installé
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 n'est pas installé. Veuillez l'installer d'abord."
    exit 1
fi

echo "✅ Python3 trouvé: $(python3 --version)"

# Créer l'environnement virtuel s'il n'existe pas
if [ ! -d "venv" ]; then
    echo "📦 Création de l'environnement virtuel..."
    python3 -m venv venv
fi

# Activer l'environnement virtuel
echo "🔌 Activation de l'environnement virtuel..."
source venv/bin/activate

# Mettre à jour pip
echo "⬆️  Mise à jour de pip..."
python3 -m pip install --upgrade pip

# Installer les dépendances
echo "📦 Installation des packages..."
python3 -m pip install -r requirements.txt

echo ""
echo "✅ Installation terminée !"
echo ""
echo "Pour utiliser l'environnement virtuel :"
echo "  source venv/bin/activate"
echo ""
echo "Pour exécuter le pipeline :"
echo "  source venv/bin/activate"
echo "  cd flows"
echo "  python3 orchestration.py"
echo ""
