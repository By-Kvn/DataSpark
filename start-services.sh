#!/bin/bash

# Script pour démarrer tous les services nécessaires
echo "🚀 Démarrage des services Docker..."

# Vérifier que Docker est installé et en cours d'exécution
if ! command -v docker &> /dev/null; then
    echo "❌ Docker n'est pas installé. Veuillez l'installer d'abord."
    echo "   Télécharger depuis : https://www.docker.com/products/docker-desktop"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker n'est pas en cours d'exécution."
    echo ""
    echo "   Sur macOS :"
    echo "   1. Ouvrir Docker Desktop depuis Applications"
    echo "   2. Attendre que l'icône Docker soit verte dans la barre de menu"
    echo "   3. Relancer ce script"
    echo ""
    exit 1
fi

# Démarrer les services
echo "📦 Démarrage de MinIO et Prefect..."
docker-compose up -d

# Attendre que les services soient prêts
echo "⏳ Attente de la disponibilité des services..."
sleep 5

# Vérifier l'état des services
echo ""
echo "📊 État des services :"
docker-compose ps

echo ""
echo "✅ Services démarrés !"
echo ""
echo "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "Prefect UI: http://localhost:4200"
echo ""
echo "Pour arrêter les services : docker-compose down"
