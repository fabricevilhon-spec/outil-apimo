# app.py

import streamlit as st
import ftplib
import io
import traceback
import pandas as pd # Import conservé pour compatibilité future

# --- CONFIGURATION ET NUMÉRO DE VERSION ---
APP_VERSION = "v2.0.0" # Fichiers scindés uniquement, retrait de la clé API, nouveau chemin FTP
FTP_HOST = "ftp.figarocms.fr"
FTP_USER = "apimo-auto-fab"
FTP_CONFIG_PATH = "/data/ftp/forge/apimoV3/CONFIG"

# --- FONCTIONS TECHNIQUES FTP ---

def connect_ftp(host, user, password):
    # RETOUR À LA FONCTION ORIGINALE
    try:
        ftp = ftplib.FTP_TLS(host, timeout=60)
        ftp.sendcmd('USER ' + user)
        ftp.sendcmd('PASS ' + password)
        return ftp
    except ftplib.all_errors as e:
        st.error(f"La connexion FTP a échoué : {e}")
        return None

def check_id_for_site(ftp, agency_id, site):
    """
    Scanne les fichiers CSV sur le FTP pour trouver l'ID.
    Retourne : Liste de tuples (chemin_fichier, mode_contact)
    """
    if site == 'figaro':
        files_to_check = ['apimo_11.csv', 'apimo_12.csv', 'apimo_13.csv']
    elif site == 'proprietes':
        files_to_check = ['apimo_31.csv', 'apimo_32.csv', 'apimo_33.csv']
    else:
        return []

    agency_id_str = str(agency_id)
    found_results = []

    for filename in files_to_check:
        try:
            ftp.cwd(FTP_CONFIG_PATH)

            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)

            content = r.getvalue().decode('utf-8', errors='ignore')
            for line in content.splitlines():
                if line.strip().startswith(agency_id_str + ','):
                    parts = line.strip().split(',')
                    contact_mode = parts[-1] if len(parts) >= 4 else '?'
                    found_results.append((f"{FTP_CONFIG_PATH}/{filename}", contact_mode))
                    break
        except Exception: pass

    return found_results

# --- FONCTIONS D'ACTION (CRUD) ---

def ajouter_client(ftp, agency_id, site, contact_mode):
    if site == 'figaro':
        login, prefix, indices = '694', 'apimo_1', ['1', '2', '3']
    elif site == 'proprietes':
        login, prefix, indices = '421', 'apimo_3', ['1', '2', '3']
    else:
        st.error("Site non valide."); return

    agency_id_str = str(agency_id)
    new_line_record = f"{agency_id_str},{login},agency,{contact_mode}"

    def append_content_robust(ftp_filename, new_record):
        ftp.cwd(FTP_CONFIG_PATH)
        lines = []
        try:
            content_in_memory = io.BytesIO()
            ftp.retrbinary(f'RETR {ftp_filename}', content_in_memory.write)
            content_decoded = content_in_memory.getvalue().decode('utf-8', errors='ignore')
            lines = [line for line in content_decoded.splitlines() if line.strip()]
        except ftplib.error_perm: pass
        lines.append(new_record)
        new_content = "\n".join(lines)
        content_to_upload = io.BytesIO(new_content.encode('utf-8'))
        ftp.cwd(FTP_CONFIG_PATH)
        ftp.storbinary(f'STOR {ftp_filename}', content_to_upload)
        st.info(f"Fichier mis à jour : {FTP_CONFIG_PATH}/{ftp_filename}")

    st.write(f"Analyse des fichiers scindés ({prefix}...) pour le site '{site}'...")
    ftp.cwd(FTP_CONFIG_PATH)

    line_counts = {}
    already_exists = False
    found_in_file = ""

    # Scan préventif anti-doublon via lecture directe
    for i in indices:
        filename = f"{prefix}{i}.csv"
        content_in_memory = io.BytesIO()
        try:
            # On essaie de lire le fichier directement
            ftp.retrbinary(f'RETR {filename}', content_in_memory.write)
            content_str = content_in_memory.getvalue().decode('utf-8', errors='ignore')
            lines = [line for line in content_str.splitlines() if line.strip()]

            for line in lines:
                if line.startswith(agency_id_str + ','):
                    already_exists = True
                    found_in_file = filename
                    break
            if already_exists: break
            line_counts[filename] = len(lines)

        except ftplib.error_perm:
            # Si le fichier n'existe pas, l'erreur est normale, on le compte à 0
            line_counts[filename] = 0

    if already_exists:
        st.warning(f"⚠️ Action annulée : L'ID {agency_id} a été trouvé dans **{found_in_file}**.")
    elif line_counts:
        smallest_file = min(line_counts, key=line_counts.get)
        st.info(f"Le fichier le plus léger est : {smallest_file} ({line_counts[smallest_file]} lignes). Mise à jour...")
        append_content_robust(smallest_file, new_line_record)
    else: st.error("Impossible de trouver les fichiers sur le serveur.")

def supprimer_client(ftp, agency_id, site):
    if site == 'figaro':
        files_to_check = ['apimo_11.csv', 'apimo_12.csv', 'apimo_13.csv']
    elif site == 'proprietes':
        files_to_check = ['apimo_31.csv', 'apimo_32.csv', 'apimo_33.csv']
    else: st.error(f"Site '{site}' non valide pour la suppression."); return

    agency_id_str, found = str(agency_id), False
    for filename in files_to_check:
        try:
            ftp.cwd(FTP_CONFIG_PATH)
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
                ftp.cwd(FTP_CONFIG_PATH)
                ftp.storbinary(f'STOR {filename}', content_io)
                st.info(f"ID {agency_id_str} supprimé dans {FTP_CONFIG_PATH}/{filename}")
        except Exception: pass
    if not found: st.warning(f"L'ID d'agence {agency_id_str} n'a été trouvé dans aucun fichier du site '{site}'.")

def modifier_client(ftp, agency_id, site, new_contact_mode):
    if site == 'figaro':
        files_to_check = ['apimo_11.csv', 'apimo_12.csv', 'apimo_13.csv']
    elif site == 'proprietes':
        files_to_check = ['apimo_31.csv', 'apimo_32.csv', 'apimo_33.csv']
    else: st.error(f"Site '{site}' non valide pour la modification."); return
    agency_id_str, found_and_modified = str(agency_id), False
    for filename in files_to_check:
        try:
            ftp.cwd(FTP_CONFIG_PATH)
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
                    if len(parts) >= 4:
                        new_line = f"{parts[0]},{parts[1]},{parts[2]},{new_contact_mode}"
                        new_lines.append(new_line)
                        file_was_modified = True
                        found_and_modified = True
                    else: new_lines.append(line)
                else: new_lines.append(line)
            if file_was_modified:
                new_content = "\n".join(new_lines)
                content_io = io.BytesIO(new_content.encode('utf-8'))
                ftp.cwd(FTP_CONFIG_PATH)
                ftp.storbinary(f'STOR {filename}', content_io)
                st.info(f"ID {agency_id_str} modifié dans {FTP_CONFIG_PATH}/{filename}")
        except Exception: pass
    if not found_and_modified: st.warning(f"L'ID d'agence {agency_id_str} n'a pas été trouvé pour modification dans les fichiers du site '{site}'.")

def verifier_parametrage_ftp(ftp, agency_id, site_choice):
    st.info(f"Recherche de l'ID d'agence '{agency_id}' sur le FTP...")
    
    results_figaro = check_id_for_site(ftp, agency_id, 'figaro')
    results_proprietes = check_id_for_site(ftp, agency_id, 'proprietes')
    all_results = results_figaro + results_proprietes
    
    if all_results:
        st.success(f"L'ID d'agence '{agency_id}' est présent :")
        for file_path, mode in all_results:
            mode_text = "Email Agence (0)" if mode == '0' else "Email Négociateur (1)" if mode == '1' else f"Valeur inconnue ({mode})"
            st.write(f"- Dans **{file_path}** avec le mode : **{mode_text}**")
    else:
        st.info(f"L'ID d'agence '{agency_id}' n'a été trouvé dans aucun fichier CSV.")

# --- INTERFACE UTILISATEUR ---
st.title("Outil de gestion des flux Apimo")

col1, col2 = st.columns(2)

with col1:
    action = st.radio("Action :", ('Ajouter', 'Supprimer', 'Vérifier', 'Modifier le mode de contact'))
    agency_id_input = st.text_input("Agency ID :")
    ftp_password = st.text_input("Mot de passe FTP :", type="password", help="Requis pour accéder aux fichiers.")

with col2:
    site_choice = st.radio("Site(s) :", ('Figaro Immobilier', 'Propriétés Le Figaro', 'Les deux'))
    contact_mode_options = {'Email Agence (0)': 0, 'Email Négociateur (1)': 1}
    
    if action == 'Ajouter' or action == 'Modifier le mode de contact':
        contact_mode = st.selectbox("Mode de contact :", options=list(contact_mode_options.keys()))
    else:
        contact_mode = None

# --- EXÉCUTION ---
if st.button("Exécuter"):
    agency_id = agency_id_input.strip()
    if not agency_id:
        st.error("L'Agency ID est obligatoire.")
    elif not ftp_password:
        st.error("Le mot de passe FTP est obligatoire.")
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
                    
                    if action == 'Vérifier':
                        verifier_parametrage_ftp(ftp, agency_id, site_choice)

                    elif action == 'Ajouter':
                        for site_code in sites_to_process:
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Traitement : {display_name}")
                            existing = check_id_for_site(ftp, agency_id, site_code)
                            if existing:
                                st.warning(f"ID {agency_id} déjà configuré pour {display_name}.")
                                continue
                            ajouter_client(ftp, agency_id, site_code, contact_mode_options[contact_mode])

                    elif action == 'Supprimer':
                        for site_code in sites_to_process:
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Suppression : {display_name}")
                            supprimer_client(ftp, agency_id, site_code)

                    elif action == 'Modifier le mode de contact':
                        for site_code in sites_to_process:
                            display_name = site_display_names.get(site_code, site_code.upper())
                            st.subheader(f"Modification : {display_name}")
                            modifier_client(ftp, agency_id, site_code, contact_mode_options[contact_mode])
                        
                st.success("Opération terminée.")
        except Exception:
            st.error("Une erreur inattendue est survenue.")
            st.code(traceback.format_exc())
        finally:
            if ftp:
                try: ftp.quit()
                except: pass

st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8em;'>Version {APP_VERSION}</div>", unsafe_allow_html=True)