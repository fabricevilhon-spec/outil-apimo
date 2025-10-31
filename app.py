# app.py

import streamlit as st
import ftplib
import io
import traceback
import pandas as pd

# --- CONFIGURATION ET NUMÉRO DE VERSION ---
APP_VERSION = "v1.5.0" # Ajout de l'affichage du mode de contact lors des vérifications
FTP_HOST = "ftp.figarocms.fr"
FTP_USER = "apimo-auto-fab"

# --- LOGIQUE MÉTIER ---

def connect_ftp(host, user, password):
    # ... (inchangé)
    try:
        ftp = ftplib.FTP_TLS(host, timeout=60)
        ftp.sendcmd('USER ' + user)
        ftp.sendcmd('PASS ' + password)
        return ftp
    except ftplib.all_errors as e:
        st.error(f"La connexion FTP a échoué : {e}")
        return None

# --- MODIFICATION DE LA FONCTION HELPER POUR EXTRAIRE LE MODE DE CONTACT ---
def check_id_for_site(ftp, agency_id, site):
    """
    Vérifie si un ID existe pour un site donné.
    Retourne une LISTE de TUPLES (chemin_fichier, mode_contact) où il a été trouvé.
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
            if path != "/": ftp.cwd(path)
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            for line in r.getvalue().decode('utf-8', errors='ignore').splitlines():
                if line.strip().startswith(agency_id_str + ','):
                    # On a trouvé la ligne, on extrait le mode de contact
                    parts = line.strip().split(',')
                    contact_mode = parts[-1] if len(parts) >= 5 else '?'
                    found_results.append((f"{path}/{filename}", contact_mode))
                    break # On passe au fichier suivant
        except Exception:
            pass
            
    return found_results

# ... les fonctions ajouter_client et supprimer_client restent inchangées ...
def ajouter_client(ftp, agency_id, site, contact_mode):
    if site == 'figaro':
        login, global_file, prefix, indices = '694', 'apimo_1.csv', 'apimo_1', ['1', '2', '3']
    elif site == 'proprietes':
        login, global_file, prefix, indices = '421', 'apimo_3.csv', 'apimo_3', ['1', '2', '3']
    else:
        st.error("Site non valide."); return
    new_line_record = f"{agency_id},{login},df93c3658a012b239ff59ccee0536f592d0c54b7,agency,{contact_mode}"
    path_global, path_split = "All", "/"
    def append_content_robust(ftp_path, ftp_filename, new_record):
        ftp.cwd("/")
        if ftp_path != "/": ftp.cwd(ftp_path)
        lines = []
        try:
            content_in_memory = io.BytesIO()
            ftp.retrbinary(f'RETR {ftp_filename}', content_in_memory.write)
            lines = [line for line in content_in_memory.getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()]
        except ftplib.error_perm: pass
        lines.append(new_record)
        new_content = "\n".join(lines)
        content_to_upload = io.BytesIO(new_content.encode('utf-8'))
        ftp.cwd("/")
        if ftp_path != "/": ftp.cwd(ftp_path)
        ftp.storbinary(f'STOR {ftp_filename}', content_to_upload)
        st.info(f"Fichier mis à jour : {ftp_path}/{ftp_filename}")
    append_content_robust(path_global, global_file, new_line_record)
    st.info(f"Analyse des fichiers scindés pour le site '{site}'...")
    ftp.cwd(path_split)
    nlst = ftp.nlst()
    line_counts = {}
    for i in indices:
        filename = f"{prefix}{i}.csv"
        if filename in nlst:
            content_in_memory = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', content_in_memory.write)
            num_lines = sum(1 for line in content_in_memory.getvalue().decode('utf-8', errors='ignore').splitlines() if line)
            line_counts[filename] = num_lines
        else:
            line_counts[filename] = 0
    smallest_file = min(line_counts, key=line_counts.get)
    st.info(f"Le fichier le plus léger est : {smallest_file} ({line_counts[smallest_file]} lignes). Mise à jour...")
    append_content_robust(path_split, smallest_file, new_line_record)

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
    # ... (inchangé)
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


# --- MODIFICATION DE LA FONCTION DE VÉRIFICATION GLOBALE ---
def verifier_client(ftp, agency_id):
    st.info(f"Recherche globale de l'ID d'agence : {agency_id}...")
    
    # On utilise notre fonction helper pour chaque site et on combine les résultats
    results_figaro = check_id_for_site(ftp, agency_id, 'figaro')
    results_proprietes = check_id_for_site(ftp, agency_id, 'proprietes')
    all_results = results_figaro + results_proprietes

    if all_results:
        st.success(f"L'ID d'agence '{agency_id}' est déjà paramétré :")
        for file_path, mode in all_results:
            mode_text = "Email Agence (0)" if mode == '0' else "Email Négociateur (1)" if mode == '1' else f"Valeur inconnue ({mode})"
            st.write(f"- Dans **{file_path}** avec le mode : **{mode_text}**")
    else:
        st.info(f"L'ID d'agence '{agency_id}' n'a été trouvé dans aucun fichier.")


# --- INTERFACE UTILISATEUR AVEC STREAMLIT ---
st.title("Outil de gestion des flux Apimo")
col1, col2 = st.columns(2)
with col1:
    action = st.radio("Choisissez une action :", ('Ajouter', 'Supprimer', 'Vérifier', 'Modifier'))
    agency_id = st.text_input("Agency ID :")
    ftp_password = st.text_input("Mot de passe FTP :", type="password")
with col2:
    site_choice = st.radio("Site(s) concerné(s) :", ('Figaro Immobilier', 'Propriétés Le Figaro', 'Les deux'))
    contact_mode_options = {'Email Agence (0)': 0, 'Email Négociateur (1)': 1}
    contact_mode = st.selectbox("Mode de contact :", options=list(contact_mode_options.keys()), help="Pour l'ajout ou la modification, définit la nouvelle valeur.")
if st.button("Exécuter"):
    if not agency_id or not ftp_password:
        st.error("L'Agency ID et le Mot de passe sont obligatoires.")
    else:
        ftp = None
        try:
            with st.spinner("Connexion au serveur FTP..."):
                ftp = connect_ftp(FTP_HOST, FTP_USER, ftp_password)
            if ftp:
                st.success("Connexion réussie.")
                sites_to_process = []
                if site_choice == 'Figaro Immobilier': sites_to_process.append('figaro')
                elif site_choice == 'Propriétés Le Figaro': sites_to_process.append('proprietes')
                elif site_choice == 'Les deux': sites_to_process.extend(['figaro', 'proprietes'])
                
                with st.spinner(f"Opération '{action}' en cours..."):
                    if action == 'Ajouter':
                        for site_code in sites_to_process:
                            st.subheader(f"Traitement pour le site : {site_code.upper()}")
                            # MODIFICATION : On gère le retour détaillé de la fonction de vérification
                            existing_results = check_id_for_site(ftp, agency_id, site_code)
                            if existing_results:
                                st.warning(f"L'ID {agency_id} existe déjà pour le site '{site_code}'. Ajout ignoré.")
                                for file_path, mode in existing_results:
                                    mode_text = "Email Agence (0)" if mode == '0' else "Email Négociateur (1)" if mode == '1' else f"Valeur inconnue ({mode})"
                                    st.write(f"- Trouvé dans **{file_path}** avec le mode : **{mode_text}**")
                                continue
                            ajouter_client(ftp, agency_id, site_code, contact_mode_options[contact_mode])
                    
                    elif action == 'Supprimer':
                        # ... inchangé
                        for site_code in sites_to_process:
                            st.subheader(f"Traitement pour le site : {site_code.upper()}")
                            supprimer_client(ftp, agency_id, site_code)
                            
                    elif action == 'Vérifier':
                        # ... inchangé
                        verifier_client(ftp, agency_id)
                        
                    elif action == 'Modifier':
                        # ... inchangé
                        for site_code in sites_to_process:
                            st.subheader(f"Traitement pour le site : {site_code.upper()}")
                            modifier_client(ftp, agency_id, site_code, contact_mode_options[contact_mode])

                st.success("Opération terminée.")
        except Exception:
            st.error("Une erreur inattendue est survenue.")
            st.code(traceback.format_exc())
        finally:
            if ftp:
                ftp.quit()

st.markdown(f"<div style='text-align: center; color: grey; font-size: 0.8em;'>Version {APP_VERSION}</div>", unsafe_allow_html=True)
