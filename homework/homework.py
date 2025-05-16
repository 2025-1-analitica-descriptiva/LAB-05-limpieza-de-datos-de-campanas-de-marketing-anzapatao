"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import numpy as np
from pathlib import Path
from zipfile import ZipFile
import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """    
    input_path = Path("files/input/")
    output_path = Path("files/output/")
    output_path.mkdir(parents=True, exist_ok=True)

    # Listas para almacenar los dataframes procesados
    client_dfs = []
    campaign_dfs = []
    economics_dfs = []

    # Obtener todos los archivos zip en la carpeta input
    zip_files = glob.glob(str(input_path / "bank-marketing-campaing-*.csv.zip"))
    
    # Procesar cada archivo zip
    for zip_file in zip_files:
        with ZipFile(zip_file) as zf:
            # Obtenemos la lista de archivos dentro del ZIP
            file_list = zf.namelist()
            
            # Verificamos que haya al menos un archivo en el ZIP
            if not file_list:
                print(f"El archivo {zip_file} está vacío")
                continue
            
            # Tomamos el primer archivo (asumiendo que solo hay uno por ZIP)
            csv_name = file_list[0]
            
            # Leer el archivo dentro del zip
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)
                
                # Extraer las columnas para client.csv
                client_cols = ['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']
                client_cols = [col for col in client_cols if col in df.columns]
                
                if client_cols:
                    df_client = df[client_cols].copy()
                    
                    # Aplicar transformaciones
                    if 'job' in df_client.columns:
                        df_client["job"] = df_client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
                    if 'education' in df_client.columns:
                        df_client["education"] = df_client["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
                    if 'credit_default' in df_client.columns:
                        df_client["credit_default"] = df_client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
                    if 'mortgage' in df_client.columns:
                        df_client["mortgage"] = df_client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
                    
                    client_dfs.append(df_client)
                
                # Extraer las columnas para campaign.csv
                # Nota: previous_campaing_contacts era el nombre correcto, pero de acuerdo con el test, debe ser previous_campaign_contacts
                campaign_cols = ['client_id', 'number_contacts', 'contact_duration', 
                                'previous_campaign_contacts', 'pdays', 'previous_outcome', 
                                'campaign_outcome', 'day', 'month']
                
                # También verificamos si existe como previous_campaing_contacts (con error ortográfico)
                if 'previous_campaing_contacts' in df.columns and 'previous_campaign_contacts' not in df.columns:
                    df = df.rename(columns={'previous_campaing_contacts': 'previous_campaign_contacts'})
                
                campaign_cols = [col for col in campaign_cols if col in df.columns]
                
                if campaign_cols:
                    df_campaign = df[campaign_cols].copy()
                    
                    # Aplicar transformaciones
                    if 'previous_outcome' in df_campaign.columns:
                        df_campaign["previous_outcome"] = df_campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
                    if 'campaign_outcome' in df_campaign.columns:
                        df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
                    
                    # Crear last_contact_date (en vez de last_contact_day)
                    if 'day' in df_campaign.columns and 'month' in df_campaign.columns:
                        # Mapeo de abreviaturas de mes a número
                        month_map = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
                                     'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}
                        
                        # Convertir mes a formato numérico
                        month_vals = df_campaign["month"].str.lower().map(month_map)
                        
                        # Asegurar que day es string y tiene dos dígitos
                        day_vals = df_campaign["day"].astype(str).str.zfill(2)
                        
                        # Crear fecha en formato YYYY-MM-DD
                        df_campaign["last_contact_date"] = "2022-" + month_vals + "-" + day_vals
                        
                        # Eliminar columnas originales
                        df_campaign.drop(['day', 'month'], axis=1, inplace=True)
                    
                    campaign_dfs.append(df_campaign)
                
                # Extraer las columnas para economics.csv
                economics_cols = ['client_id', 'cons_price_idx', 'euribor_three_months']
                
                # Verificar nombres alternativos
                col_mappings = {
                    'const_price_idx': 'cons_price_idx',
                    'euribor3m': 'euribor_three_months'
                }
                
                for old_col, new_col in col_mappings.items():
                    if old_col in df.columns and new_col not in df.columns:
                        df = df.rename(columns={old_col: new_col})
                
                economics_cols = [col for col in economics_cols if col in df.columns]
                
                if economics_cols:
                    df_economics = df[economics_cols].copy()
                    economics_dfs.append(df_economics)

    # Combinar todos los dataframes y guardar archivos CSV
    if client_dfs:
        final_client_df = pd.concat(client_dfs, ignore_index=True).drop_duplicates(subset=['client_id'])
        final_client_df.to_csv(output_path / "client.csv", index=False)
    
    if campaign_dfs:
        final_campaign_df = pd.concat(campaign_dfs, ignore_index=True).drop_duplicates(subset=['client_id'])
        final_campaign_df.to_csv(output_path / "campaign.csv", index=False)
    
    if economics_dfs:
        final_economics_df = pd.concat(economics_dfs, ignore_index=True).drop_duplicates(subset=['client_id'])
        final_economics_df.to_csv(output_path / "economics.csv", index=False)

if __name__ == "__main__":
    clean_campaign_data()