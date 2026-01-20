# Guide de dépannage

## ❌ Erreur : "Cannot connect to the Docker daemon"

### Solution 1 : Démarrer Docker Desktop manuellement

1. **Ouvrir Docker Desktop**
   - Appuyer sur `Cmd + Espace` (Spotlight)
   - Taper "Docker" et appuyer sur Entrée
   - OU aller dans Applications → Docker

2. **Attendre que Docker soit prêt**
   - L'icône Docker apparaît dans la barre de menu (en haut à droite)
   - Attendre qu'elle soit **verte** (pas orange/rouge)
   - Cela peut prendre 30 secondes à 2 minutes

3. **Vérifier**
   ```bash
   docker ps
   ```
   Cette commande doit retourner une liste (même vide) sans erreur.

### Solution 2 : Vérifier l'installation de Docker

Si Docker Desktop n'apparaît pas dans Applications :

1. **Vérifier l'installation**
   ```bash
   which docker
   ```
   Si rien n'apparaît, Docker n'est pas installé.

2. **Installer Docker Desktop**
   - Télécharger depuis : https://www.docker.com/products/docker-desktop
   - Installer le fichier `.dmg`
   - Glisser Docker dans Applications
   - Lancer Docker Desktop

### Solution 3 : Redémarrer Docker Desktop

Si Docker est installé mais ne démarre pas :

1. **Quitter Docker Desktop complètement**
   - Clic droit sur l'icône Docker dans la barre de menu
   - Choisir "Quit Docker Desktop"

2. **Relancer Docker Desktop**
   - Ouvrir depuis Applications
   - Attendre que l'icône soit verte

### Solution 4 : Vérifier les permissions

Parfois Docker nécessite des permissions supplémentaires :

1. Aller dans **Préférences Système** → **Sécurité et confidentialité**
2. Vérifier que Docker a les permissions nécessaires
3. Redémarrer Docker Desktop si besoin

## ✅ Vérification que Docker fonctionne

Une fois Docker Desktop démarré, tester :

```bash
# Vérifier que Docker répond
docker ps

# Vérifier la version
docker --version

# Vérifier Docker Compose
docker-compose --version
```

Toutes ces commandes doivent fonctionner sans erreur.

## 🚀 Après avoir démarré Docker

Une fois Docker Desktop démarré et l'icône verte :

```bash
cd /Users/kevin/Desktop/BigData-ex
docker-compose up -d
```

Puis vérifier que les services sont démarrés :

```bash
docker ps
```

Tu devrais voir 4 conteneurs en cours d'exécution.
