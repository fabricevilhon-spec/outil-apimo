# app.py

import streamlit as st
import ftplib
import io
import traceback
import pandas as pd

# --- CONFIGURATION ---
FTP_HOST = "ftp.figarocms.fr"
FTP_USER = "fvilhon"

# --- LOGIQUE MÉTIER (Nos fonctions sont presque inchangées) ---

def connect_ftp(host, user, password):
    try:
        ftp = ftplib.FTP(host, timeout=60)
        ftp.login(user, password)
        return ftp
    except ftplib.all_errors as e:
        st.error(f"La connexion FTP a échoué : {e}") # Remplacer print par st.error
        return None

def ajouter_client(ftp, agency_id, site, contact_mode):
    # Cette fonction est identique à la version finale de notre script Colab
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
    st.info("Analyse des fichiers scindés pour trouver le plus léger...")
    ftp.cwd(path_split)
    nlst = ftp.nlst()
    line_counts = {f"{prefix}{i}.csv": sum(1 for line in io.BytesIO(ftp.retrbinary(f"RETR {f'{prefix}{i}.csv'}", io.BytesIO().write) or b'').getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()) if f"{prefix}{i}.csv" in nlst else 0 for i in indices}
    smallest_file = min(line_counts, key=line_counts.get)
    st.info(f"Le fichier avec le moins de lignes est : {smallest_file} ({line_counts[smallest_file]} lignes). Mise à jour...")
    append_content_robust(path_split, smallest_file, new_line_record)

def supprimer_client(ftp, agency_id, site):
    # Cette fonction est aussi identique
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

def verifier_client(ftp, agency_id):
    # Cette fonction est aussi identique
    st.info(f"Recherche de l'ID d'agence : {agency_id}...")
    files_to_check = [("All", 'apimo_1.csv'), ("All", 'apimo_3.csv'), ("/", 'apimo_11.csv'), ("/", 'apimo_12.csv'), ("/", 'apimo_13.csv'), ("/", 'apimo_31.csv'), ("/", 'apimo_32.csv'), ("/", 'apimo_33.csv')]
    agency_id_str, found_in_files = str(agency_id), []
    for path, filename in files_to_check:
        try:
            ftp.cwd("/")
            if path != "/": ftp.cwd(path)
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            if r.getbuffer().nbytes == 0: continue
            lines = [line.strip() for line in r.getvalue().decode('utf-8', errors='ignore').splitlines() if line.strip()]
            for line in lines:
                if line.startswith(agency_id_str + ','):
                    found_in_files.append(f"{path}/{filename}")
                    break
        except Exception: pass
    if found_in_files:
        st.success(f"L'ID d'agence '{agency_id_str}' est déjà paramétré dans les fichiers suivants :")
        for file_path in found_in_files:
            st.write(f"- {file_path}") # Remplacer print par st.write
    else:
        st.info(f"L'ID d'agence '{agency_id_str}' n'a été trouvé dans aucun fichier.")


# --- INTERFACE UTILISATEUR AVEC STREAMLIT ---

st.title("Outil de gestion des flux Apimo") # Le titre de la page web

# Utilisation des colonnes pour un affichage plus propre
col1, col2 = st.columns(2)

with col1:
    action = st.radio("Choisissez une action :", ('Ajouter', 'Supprimer', 'Vérifier'))
    agency_id = st.text_input("Agency ID :")
    # On cache le champ mot de passe
    ftp_password = st.text_input("Mot de passe FTP :", type="password")

with col2:
    site_options = {'Figaro Immobilier': 'figaro', 'Propriétés Le Figaro': 'proprietes'}
    site = st.selectbox("Site concerné :", options=list(site_options.keys()))
    
    contact_mode_options = {'Email Agence (0)': 0, 'Email Négociateur (1)': 1}
    contact_mode = st.selectbox("Mode de contact :", options=list(contact_mode_options.keys()))

# Le bouton d'exécution
if st.button("Exécuter"):
    if not agency_id or not ftp_password:
        st.error("L'Agency ID et le Mot de passe sont obligatoires.")
    else:
        ftp = None
        try:
            # Affiche une barre de progression pendant la connexion
            with st.spinner("Connexion au serveur FTP..."):
                ftp = connect_ftp(FTP_HOST, FTP_USER, ftp_password)
            
            if ftp:
                st.success("Connexion réussie.")
                # Barre de progression pour l'action
                with st.spinner(f"Opération '{action}' en cours..."):
                    if action == 'Ajouter':
                        ajouter_client(ftp, agency_id, site_options[site], contact_mode_options[contact_mode])
                    elif action == 'Supprimer':
                        supprimer_client(ftp, agency_id, site_options[site])
                    elif action == 'Vérifier':
                        verifier_client(ftp, agency_id)
                st.success("Opération terminée.")
        except Exception:
            st.error("Une erreur inattendue est survenue.")
            st.code(traceback.format_exc()) # Affiche l'erreur complète
        finally:
            if ftp:
                ftp.quit()
