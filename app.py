# app.py

import streamlit as st
import ftplib
import io
import traceback
import pandas as pd # Import conservé pour compatibilité

# --- CONFIGURATION ET NUMÉRO DE VERSION ---
APP_VERSION = "v1.6.2" # Correction syntaxe longue ligne
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
            if path != "/": ftp.cwd(path)
            
            r = io.BytesIO()
            ftp.retrbinary(f'RETR {filename}', r.write)
            r.seek(0)
            
            # Lecture ligne par ligne pour trouver l'ID
            for line in r.getvalue().decode('utf-8', errors='ignore').splitlines():
                if line.strip().startswith(agency_id_str + ','):
                    parts = line.strip().split(',')
                    contact_mode = parts[-1] if len(parts) >= 5 else '?'
                    found_results.append((f"{path}/{filename}", contact_mode))
                    break 
        except Exception:
            pass 
            
    return found_results

def ajouter_client(ftp, agency_id, site, contact_mode, add_to_global=True, add_to_split=True):
    """
    Ajoute le client avec une double vérification de sécurité.
    """
    if site == 'figaro':
        login, global_file, prefix, indices = '694', 'apimo_1.csv', 'apimo_1', ['1', '2', '3']
    elif site == 'proprietes':
        login, global_file, prefix, indices = '421', 'apimo_3.csv', 'apimo_3', ['1', '2', '3']
    else:
        st.error("Site non valide."); return

    agency_id_str = str(agency_id)
    # Le hash est codé en dur
    new_line_record = f"{agency_id_str},{login},df93c3658a012b239ff59ccee0536f592d0c54b7,agency,{contact_mode}"
    path_global, path_split = "All", "/"

    # Fonction utilitaire interne pour écrire
    def append_content_robust(ftp_path, ftp_filename, new_record):
        ftp.cwd("/")
        if ftp_path != "/": ftp.cwd(ftp_path)
        lines = []
        try