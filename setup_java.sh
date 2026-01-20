#!/bin/bash

# Script pour configurer JAVA_HOME pour PySpark
# Usage: source setup_java.sh

echo "🔧 Configuration de JAVA_HOME pour PySpark..."

# Trouver Java installé via Homebrew
JAVA_PATH=$(brew --prefix openjdk@17 2>/dev/null)

if [ -z "$JAVA_PATH" ]; then
    echo "❌ Java n'est pas installé. Installation..."
    brew install openjdk@17
    JAVA_PATH=$(brew --prefix openjdk@17)
fi

# Configurer JAVA_HOME
export JAVA_HOME="$JAVA_PATH/libexec/openjdk.jdk/Contents/Home"

if [ -d "$JAVA_HOME" ]; then
    echo "✅ JAVA_HOME configuré: $JAVA_HOME"
    echo "✅ Version Java:"
    $JAVA_HOME/bin/java -version
    echo ""
    echo "💡 Pour utiliser PySpark, exécutez:"
    echo "   export JAVA_HOME=\"$JAVA_HOME\""
    echo "   source venv/bin/activate"
    echo "   python script/benchmark_pandas_vs_spark.py"
else
    echo "❌ Erreur: JAVA_HOME introuvable à $JAVA_HOME"
    exit 1
fi
