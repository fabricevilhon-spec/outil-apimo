# app.py

import streamlit as st
import ftplib
import io
import traceback
import pandas as pd # Import conservé pour compatibilité

# --- CONFIGURATION ET NUMÉRO DE VERSION ---
APP_VERSION = "v1.6.1" # Ajout sécurité anti-doublon stricte dans les fichiers Split
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
    new_line_record = f"{agency_id_str},{login},df93c3658a012b239ff59ccee0536f592d0c54b7,agency,{contact_mode}"
    path_global, path_split = "All", "/"

    # Fonction utilitaire interne pour écrire
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

    # 1. Ajout au fichier GLOBAL
    if add_to_global:
        st.write(f"Ajout au fichier Global ({global_file})...")
        append_content_robust(path_global, global_file, new_line_record)
    else:
        st.info(f"Le client est déjà présent dans le fichier Global ({global_file}). Ajout ignoré.")

    # 2. Ajout au fichier SPLIT (Esclave) AVEC SÉCURITÉ RENFORCÉE
    if add_to_split:
        st.write(f"Analyse des fichiers scindés ({prefix}...) pour le site '{site}'...")
        ftp.cwd(path_split)
        nlst = ftp.nlst()
        line_counts = {}
        already_exists_in_split = False
        found_in_file = ""

        # On parcourt TOUS les fichiers scindés pour compter les lignes ET vérifier si l'ID existe déjà
        for i in indices:
            filename = f"{prefix}{i}.csv"
            if filename in nlst:
                content_in_memory = io.BytesIO()
                ftp.retrbinary(f'RETR {filename}', content_in_memory.write)
                content_str = content_in_memory.getvalue().decode('utf-8', errors='ignore')
                lines = [line for line in content_str.splitlines() if line.strip()]
                
                # SÉCURITÉ : On vérifie si l'ID est dedans
                for line in lines:
                    if line.startswith(agency_id_str + ','):
                        already_exists_in_split = True
                        found_in_file = filename
                        break
                
                if already_exists_in_split:
                    break # Inutile de continuer à chercher, on a trouvé un doublon
                
                line_counts[filename] = len(lines)
            else:
                line_counts[filename] = 0

        if already_exists_in_split:
            st.warning(f"⚠️ Action annulée pour les fichiers scindés : L'ID {agency_id} a été trouvé dans **{found_in_file}** au moment de l'écriture. Cela évite un doublon.")
        elif line_counts:
            # Si on est ici, c'est que l'ID n'est dans AUCUN des fichiers scindés
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

def verifier_client(ftp, agency_id):
    st.info(f"Recherche globale de l'ID d'agence : {agency_id}...")
    results_figaro = check_id_for_site(ftp, agency_id, 'figaro')
    results_proprietes = check_id_for_site(ftp, agency_id, 'proprietes')
    
    all_results = results_figaro + results_proprietes
    
    if all_results:
        st.success(f"L'ID d'agence '{agency_id}' est présent :")
        for file_path, mode in all_results:
            mode_text = "Email Agence (0)" if mode == '0' else "Email Négociateur (1)" if mode == '1' else f"Valeur inconnue ({mode})"
            st.write(f"- Dans **{file_path}** avec le mode : **{mode_text}**")
            
        check_coherence(results_figaro, "Figaro Immobilier")
        check_coherence(results_proprietes, "Propriétés Le Figaro")
    else:
        st.info(f"L'ID d'agence '{agency_id}' n'a été trouvé dans aucun fichier.")

def check_coherence(results, site_name):
    if not results: return
    has_global = any("All/" in path for path, _ in results)
    has_split = any(path.startswith("/apimo") for path, _ in results)
    
    if has_global and has_split:
        st.caption(f"✅ Configuration {site_name} cohérente (Présent Global + Split).")
    elif has_global and not has_split:
        st.error(f"⚠️ Configuration {site_name} INCOMPLÈTE : Présent dans Global mais manquant dans les fichiers scindés.")
    elif not has_global and has_split:
        st.error(f"⚠️ Configuration {site_name} INCOMPLÈTE : Présent dans un