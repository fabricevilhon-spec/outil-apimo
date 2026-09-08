# Outil de gestion des flux Apimo

Application Streamlit permettant de gérer les fichiers CSV de configuration des flux Apimo (Figaro Immobilier / Propriétés Le Figaro) hébergés sur un serveur FTP. Elle permet d'ajouter, supprimer, modifier et vérifier la présence d'un ID d'agence dans les fichiers de flux.

## Fonctionnalités

- **Vérifier** : recherche un `Agency ID` dans les fichiers CSV des deux sites et affiche son mode de contact (Email Agence / Email Négociateur).
- **Ajouter** : ajoute une nouvelle ligne pour l'agence dans le fichier scindé le moins rempli (répartition de charge), en évitant les doublons.
- **Supprimer** : retire toutes les lignes correspondant à l'`Agency ID` dans l'ensemble des fichiers scindés du/des site(s) sélectionné(s).
- **Modifier le mode de contact** : met à jour le mode de contact (Email Agence / Email Négociateur) de l'agence dans tous les fichiers où elle est présente.

## Sites gérés

Tous les fichiers sont situés dans `/data/ftp/forge/apimoV3/CONFIG` sur le serveur FTP.

| Site | Fichiers scindés |
|---|---|
| Figaro Immobilier | `apimo_11.csv`, `apimo_12.csv`, `apimo_13.csv` |
| Propriétés Le Figaro | `apimo_31.csv`, `apimo_32.csv`, `apimo_33.csv` |

Chaque ligne CSV suit le format : `agency_id,login,agency,contact_mode`. La clé API n'est plus présente dans les fichiers pour des raisons de sécurité.

## Prérequis

- Python 3
- Dépendances listées dans `requirements.txt` (`streamlit`, `pandas`)

```bash
pip install -r requirements.txt
```

## Lancement

```bash
streamlit run app.py
```

## Utilisation

1. Choisir l'action à effectuer (Vérifier, Ajouter, Supprimer, Modifier le mode de contact).
2. Renseigner l'`Agency ID` de l'agence concernée.
3. Saisir le mot de passe FTP (demandé à chaque exécution, non stocké).
4. Sélectionner le(s) site(s) concerné(s).
5. Pour les actions Ajouter / Modifier, choisir le mode de contact souhaité.
6. Cliquer sur **Exécuter**.

## Notes techniques

- La connexion au serveur FTP (`ftp.figarocms.fr`) se fait en FTP_TLS avec l'utilisateur `apimo-auto-fab` et le mot de passe saisi dans l'interface ; aucun identifiant n'est stocké en clair dans le code.
- Avant tout ajout, l'application vérifie l'absence de doublon dans les fichiers scindés.
- La répartition de charge lors d'un ajout se fait en choisissant le fichier scindé comportant le moins de lignes.
- Les erreurs de connexion ou de traitement sont affichées dans l'interface, avec la trace complète en cas d'erreur inattendue.

## Version

Voir le numéro de version affiché en bas de l'interface (`APP_VERSION` dans `app.py`).
