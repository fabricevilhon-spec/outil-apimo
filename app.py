# app.py

import streamlit as st
import ftplib
import io
import traceback
import requests
import json  # NÉCESSAIRE POUR LE TÉLÉCHARGEMENT JSON
from requests.auth import HTTPBasicAuth
import pandas as pd # Import conservé pour compatibilité

# --- CONFIGURATION ET NUMÉRO DE VERSION ---
APP_VERSION = "v1.8.0" # Ajout du bouton de téléchargement JSON
FTP_HOST = "ftp.figarocms.fr"
FTP_USER = "apimo-auto-fab"

# --- LOGIQUE MÉTIER ---

def connect_ftp(host, user, password):
    try:
        ftp = ftplib.FTP_TLS(host, timeout=60)
        ftp.sendcmd('USER ' + user)
        ftp.sendcmd('PASS ' + password)
        return ftp
    except ftplib.all_errors as e:
        st.error(f"La connexion FTP a échoué : {e}")
        return None

def check_apimo_api(agency_id, site_choice, api_password):
    """
    Interroge l'API Apimo.
    Retourne : (Booléen Succès, Message Utilisateur, Données JSON ou None)
    """
    if not api_password:
        return None, "Mot de passe API non fourni.", None

    # Définition des credentials selon le site
    if site_choice == 'Figaro Immobilier':
        api_user = '694'
    elif site_choice == 'Propriétés Le Figaro':
        api_user = '421'
    else:
        api_user = '694' 

    url = f"https://api.apimo.pro/agencies/{agency_id}/properties"
    
    try:
        # Requête GET avec Basic Auth
        response = requests.get(url, auth=HTTPBasicAuth(api_user, api_password), timeout=10)
        
        # --- GESTION DES RÉPONSES ---
        
        if response.status_code == 200:
            try:
                data = response.json()
                count = data.get('total_items', 0)
                # ON RETOURNE MAINTENANT "data" EN 3ème ARGUMENT
                return True, f"Agence active et connectée. ({count} annonce{'s' if count > 1 else ''} en ligne)", data
            except Exception:
                return True, "Agence active (Code 200), mais lecture du JSON impossible.", None

        elif response.status_code == 404:
            return False, f"Agence introuvable côté Apimo (L'ID {agency_id} n'existe pas).", None

        elif response.status_code == 403:
            return False, "Accès refusé par Apimo. L'agence est probablement inactive ou le lien est coupé.", None

        elif response.status_code == 401:
            return False, f"Échec d'authentification. Le mot de passe API pour l'utilisateur '{api_user}' semble incorrect.", None

        else:
            return False, f"Erreur technique API (Code {response.status_code}).", None
            
    except requests.exceptions.Timeout:
        return False, "Le serveur Apimo met trop de temps à répondre (Timeout).", None
    except Exception as e:
        return False, f"Erreur de connexion : {str(e)}", None

def check_id_for_site(ftp, agency_id, site):
    """
    Retourne une liste de tuples (chemin_fichier, mode_contact) où l'ID est trouvé.
    """
    if site == 'figaro':
        files_to_check = [("All", 'apimo_1.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv')]
    elif site == 'proprietes':
        files_to_check = [("All", 'apimo_3.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    else:
        return []
    
    agency_id_str = str(agency_id)
    found_results = []
    
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/":
                ftp.cwd(path)
            
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            
            # Lecture ligne par ligne pour trouver l'ID
            content = r.getvalue().decode('utf-8', errors='ignore')
            for line in content.splitlines():
                if line.strip().startswith(agency_id_str + ','):
                    parts = line.strip().split(',')
                    contact_mode = parts[-1] if len(parts) >= 5 else '?'
                    
                    if path == "/":
                        clean_path = f"/{filename}"
                    else:
                        clean_path = f"{path}/{filename}"
                        
                    found_results.append((clean_path, contact_mode))
                    break 
        except Exception:
            pass 
            
    return found_results

def ajouter_client(ftp, agency_id, site, contact_mode, add_to_global=True, add_to_split=True):
    if site == 'figaro':
        login, global_file, prefix, indices = '694', 'apimo_1.csv', 'apimo_1', ['1', '2', '3']
    elif site == 'proprietes':
        login, global_file, prefix, indices = '421', 'apimo_3.csv', 'apimo_3', ['1', '2', '3']
    else:
        st.error("Site non valide.")
        return

    agency_id_str = str(agency_id)
    new_line_record = f"{agency_id_str},{login},df93c3658a012b239ff59ccee0536f592d0c54b7,agency,{contact_mode}"
    path_global, path_split = "All", "/"

    def append_content_robust(ftp_path, ftp_filename, new_record):
        ftp.cwd("/")
        if ftp_path != "/":
            ftp.cwd(ftp_path)
        lines = []
        try:
            content_in_memory = io.BytesIO()
            ftp.retrbinary(f'RETR {ftp_filename}', content_in_memory.write)
            content_decoded = content_in_memory.getvalue().decode('utf-8', errors='ignore')
            lines = [line for line in content_decoded.splitlines() if line.strip()]
        except ftplib.error_perm:
            pass
            
        lines.append(new_record)
        new_content = "\n".join(lines)
        content_to_upload = io.BytesIO(new_content.encode('utf-8'))
        ftp.cwd("/")
        if ftp_path != "/":
            ftp.cwd(ftp_path)
        ftp.storbinary(f'STOR {ftp_filename}', content_to_upload)
        st.info(f"Fichier mis à jour : {ftp_path}/{ftp_filename}")

    if add_to_global:
        st.write(f"Ajout au fichier Global ({global_file})...")
        append_content_robust(path_global, global_file, new_line_record)
    else:
        st.info(f"Le client est déjà présent dans le fichier Global ({global_file}). Ajout ignoré.")

    if add_to_split:
        st.write(f"Analyse des fichiers scindés ({prefix}...) pour le site '{site}'...")
        ftp.cwd(path_split)
        nlst = ftp.nlst()
        line_counts = {}
        already_exists_in_split = False
        found_in_file = ""

        for i in indices:
            filename = f"{prefix}{i}.csv"
            if filename in nlst:
                content_in_memory = io.BytesIO()
                ftp.retrbinary(f'RETR {filename}', content_in_memory.write)
                content_str = content_in_memory.getvalue().decode('utf-8', errors='ignore')
                lines = [line for line in content_str.splitlines() if line.strip()]
                for line in lines:
                    if line.startswith(agency_id_str + ','):
                        already_exists_in_split = True
                        found_in_file = filename
                        break
                if already_exists_in_split: break
                line_counts[filename] = len(lines)
            else:
                line_counts[filename] = 0

        if already_exists_in_split:
            st.warning(f"⚠️ Action annulée pour les fichiers scindés : L'ID {agency_id} a été trouvé dans **{found_in_file}**.")
        elif line_counts:
            smallest_file = min(line_counts, key=line_counts.get)
            st.info(f"Le fichier le plus léger est : {smallest_file} ({line_counts[smallest_file]} lignes). Mise à jour...")
            append_content_robust(path_split, smallest_file, new_line_record)
        else:
            st.error("Impossible de trouver les fichiers scindés sur le serveur.")
    else:
        st.info("La logique a déterminé que l'ID est déjà présent dans un fichier scindé.")

def supprimer_client(ftp, agency_id, site):
    if site == 'figaro':
        files_to_check = [("All", 'apimo_1.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv')]
    elif site == 'proprietes':
        files_to_check = [("All", 'apimo_3.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    else:
        st.error(f"Site '{site}' non valide pour la suppression."); return

    agency_id_str, found = str(agency_id), False
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/": ftp.cwd(path)
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            if r.getbuffer().nbytes == 0: continue
            lines = [line.strip() for line in r.getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()]
            initial_rows = len(lines)
            lines_filtered = [line for line in lines if not line.startswith(agency_id_str + ',')]
            if len(lines_filtered) < initial_rows:
                found = True
                new_content = "\n".join(lines_filtered)
                content_io = io.BytesIO(new_content.encode('utf-8'))
                ftp.cwd("/")
                if path != "/": ftp.cwd(path)
                ftp.storbinary(f'STOR {filename}', content_io)
                st.info(f"ID {agency_id_str} supprimé dans {path}/{filename}")
        except Exception: pass
    if not found: st.warning(f"L'ID d'agence {agency_id_str} n'a été trouvé dans aucun fichier du site '{site}'.")

def modifier_client(ftp, agency_id, site, new_contact_mode):
    if site == 'figaro':
        files_to_check = [("All", 'apimo_1.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv')]
    elif site == 'proprietes':
        files_to_check = [("All", 'apimo_3.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    else:
        st.error(f"Site '{site}' non valide pour la modification."); return
    agency_id_str, found_and_modified = str(agency_id), False
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/": ftp.cwd(path)
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            if r.getbuffer().nbytes == 0: continue
            lines = [line.strip() for line in r.getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()]
            new_lines = []
            file_was_modified = False
            for line in lines:
                if line.startswith(agency_id_str + ','):
                    parts = line.split(',')
                    if len(parts) >= 5:
                        new_line = f"{parts[0]},{parts[1]},{parts[2]},{parts[3]},{new_contact_mode}"
                        new_lines.append(new_line)
                        file_was_modified = True
                        found_and_modified = True
                    else:
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            if file_was_modified:
                new_content = "\n".join(new_lines)
                content_io = io.BytesIO(new_content.encode('utf-8'))
                ftp.cwd("/")
                if path != "/": ftp.cwd(path)
                ftp.storbinary(f'STOR {filename}', content_io)
                st.info(f"ID {agency_id_str} modifié dans {path}/{filename}")
        except Exception: pass
    if not found_and_modified: st.warning(f"L'ID d'agence {agency_id_str} n'a pas été trouvé pour modification dans les fichiers du site '{site}'.")

def verifier_client(ftp, agency_id, site_choice, api_password):
    st.info(f"Recherche globale de l'ID d'agence : {agency_id}...")
    
    # 1. Vérification API APIMO
    if api_password:
        st.markdown("---")
        st.subheader("📡 Statut API Apimo")
        with st.spinner("Interrogation de l'API Apimo en cours..."):
            # On récupère maintenant 3 valeurs : succès, message, ET les données JSON
            is_active, message, json_data = check_apimo_api(agency_id, site_choice, api_password)
        
        # Affichage adapté selon le résultat
        if is_active:
            st.success(f"✅ {message}")
            # --- BOUTON DE TÉLÉCHARGEMENT ---
            if json_data:
                # On convertit le JSON en texte joliment formaté
                json_str = json.dumps(json_data, indent=4, ensure_ascii=False)
                st.download_button(
                    label="📥 Télécharger le fichier JSON complet",
                    data=json_str,
                    file_name=f"apimo_{agency_id}_data.json",
                    mime="application/json",
                    help="Télécharger la réponse brute de l'API (liste des annonces)"
                )
            # --------------------------------
        elif is_active is False:
            if "introuvable" in message or "refusé" in message:
                st.error(f"❌ {message}")
            else:
                st.warning(f"⚠️ {message}")
        else:
            st.warning(f"⚠️ {message}")
        st.markdown("---")
    else:
        st.warning("ℹ️ Pas de mot de passe API fourni, vérification API ignorée.")

    # 2. Vérification FTP (Existant)
    st.subheader("📂 Statut Configuration FTP")
    results_figaro = check_id_for_site(ftp, agency_id, 'figaro')
    results_proprietes = check_id_for_site(ftp, agency_id, 'proprietes')
    all_results = results_figaro + results_proprietes
    
    if all_results:
        st.success(f"L'ID d'agence '{agency_id}' est présent dans la config FTP :")
        for file_path, mode in all_results:
            mode_text = "Email Agence (0)" if mode == '0' else "Email Négociateur (1)" if mode == '1' else f"Valeur inconnue ({mode})"
            st.write(f"- Dans **{file_path}** avec le mode : **{mode_text}**")
        check_coherence(results_figaro, "Figaro Immobilier")
        check_coherence(results_proprietes, "Propriétés Le Figaro")
    else:
        st.info(f"L'ID d'agence '{agency_id}' n'a été trouvé dans aucun fichier CSV.")

def check_coherence(results, site_name):
    if not results: return
    has_global = any("All/" in path for path, _ in results)
    has_split = any("All/" not in path for path, _ in results)
    if has_global and has_split:
        st.caption(f"✅ Configuration {site_name} cohérente (Présent Global + Split).")
    elif has_global and not has_split:
        st.error(f"⚠️ Configuration {site_name} INCOMPLÈTE : Présent dans Global mais manquant dans les fichiers scindés.")
    elif not has_global and has_split:
        st.error(f"⚠️ Configuration {site_name} INCOMPLÈTE : Présent dans un fichier scindé mais manquant dans Global.")

# --- INTERFACE UTILISATEUR AVEC STREAMLIT ---
st.title("Outil de gestion des flux Apimo")

col1, col2 = st.columns(2)
with col1:
    action = st.radio("Action :", ('Ajouter', 'Supprimer', 'Vérifier', 'Modifier le mode de contact'))
    agency_id_input = st.text_input("Agency ID :")
    ftp_password = st.text_input("Mot de passe FTP :", type="password", help="Pour se connecter aux fichiers CSV")
    
    # NOUVEAU CHAMP POUR L'API
    if action == 'Vérifier':
        api_password = st.text_input("Mot de passe API (Optionnel) :", type="password", help="Mot de passe API du provider (694 ou 421) pour vérifier si l'agence est active chez Apimo.")
    else:
        api_password = None

with col2:
    site_choice = st.radio("Site(s) :", ('Figaro Immobilier', 'Propriétés Le Figaro', 'Les deux'))
    contact_mode_options = {'Email Agence (0)': 0, 'Email Négociateur (1)': 1}
    contact_mode = st.selectbox("Mode de contact :", options=list(contact_mode_options.keys()))

if st.button("Exécuter"):
    agency_id = agency_id_input.strip()
    if not agency_id or not ftp_password:
        st.error("L'Agency ID et le Mot de passe FTP sont obligatoires.")
    else:
        ftp = None
        try:
            with st.spinner("Connexion au serveur FTP..."):
                ftp = connect_ftp(FTP_HOST, FTP_USER, ftp_password)
            if ftp:
                st.success("Connexion FTP réussie.")
                
                site_display_names = {'figaro': 'Figaro Immobilier', 'proprietes': 'Propriétés Le Figaro'}
                sites_to_process = []
                if site_choice == 'Figaro Immobilier': sites_to_process.append('figaro')
                elif site_choice == 'Propriétés Le Figaro': sites_to_process.append('proprietes')
                elif site_choice == 'Les deux': sites_to_process.extend(['figaro', 'proprietes'])
                
                with st.spinner(f"Opération '{action}' en cours..."):
                    if action == 'Ajouter':
                        for site_code in sites_to_process:
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Traitement pour le site : {display_name}")
                            existing_results = check_id_for_site(ftp, agency_id, site_code)
                            is_in_global = any("All/" in r[0] for r in existing_results)
                            is_in_split = any("All/" not in r[0] for r in existing_results)
                            if is_in_global and is_in_split:
                                st.warning(f"ID {agency_id} déjà configuré pour {display_name}.")
                                continue
                            ajouter_client(ftp, agency_id, site_code, contact_mode_options[contact_mode], add_to_global=not is_in_global, add_to_split=not is_in_split)

                    elif action == 'Supprimer':
                        for site_code in sites_to_process:
                            st.subheader(f"Suppression : {site_display_names.get(site_code)}")
                            supprimer_client(ftp, agency_id, site_code)

                    elif action == 'Modifier le mode de contact':
                        for site_code in sites_to_process:
                            st.subheader(f"Modification : {site_display_names.get(site_code)}")
                            modifier_client(ftp, agency_id, site_code, contact_mode_options[contact_mode])

                    elif action == 'Vérifier':
                        # Modification ici pour passer les arguments API
                        verifier_client(ftp, agency_id, site_choice, api_password)
                        
                st.success("Opération terminée.")
        except Exception:
            st.error("Une erreur inattendue est survenue.")
            st.code(traceback.format_exc())
        finally:
            if ftp:
                try: ftp.quit()
                except: pass

st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8em;'>Version {APP_VERSION}</div>", unsafe_allow_html=True)