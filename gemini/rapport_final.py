# -*- coding: utf-8 -*-
"""
Pipeline d'analyse complet des abonnements.

Ce script transforme les données brutes en un rapport d'analyse multi-onglets
contenant les analyses de rétention et les profils de cohortes de clients.
"""
import pandas as pd
import numpy as np

# --- CONFIGURATION ---
TRANSACTIONS_FILE = 'transaction.csv'
COUPONS_FILE = 'Table des coupons-1.xlsx - Coupons.csv'
OUTPUT_FILE = 'rapport_analyse_abonnements.xlsx'

# --- PARTIE 1 : PRÉPARATION DES DONNÉES (Fonctions regroupées) ---

def load_and_merge_data(transactions_path, coupons_path):
    """Charge, fusionne et enrichit les données initiales."""
    print("--- Étape 1: Chargement et fusion ---")
    try:
        df_trans = pd.read_csv(transactions_path, encoding='latin1')
        df_coupons = pd.read_csv(coupons_path, encoding='latin1')
        df_merged = pd.merge(df_trans, df_coupons, left_on='discount', right_on='Coupon Id', how='left')
        df_cleaned = df_merged.drop_duplicates().reset_index(drop=True)
        df_cleaned['nom_offre'] = np.where(pd.notna(df_cleaned['tm_campaign']), df_cleaned['tm_campaign'], df_cleaned['discount'])
        df_cleaned['nom_offre'] = df_cleaned['nom_offre'].fillna('Offre Standard')
        return df_cleaned
    except Exception as e:
        print(f"ERREUR CRITIQUE lors du chargement : {e}")
        return None

def group_and_repair_data(df):
    """Agrège les données et répare les dates manquantes."""
    if df is None: return None
    print("\n--- Étape 2: Agrégation et réparation des dates ---")
    agg_logic = {col: 'first' for col in df.columns if col != 'subscription_id'}
    agg_logic.update({'order_date': 'first', 'ECHEANCE_date': 'last'})
    for col in ['order_date (Année)', 'order_date (Mois)', 'order_date (Jour du mois)', 'ECHEANCE_annee', 'ECHEANCE_mois', 'ECHEANCE_jour']:
        agg_logic[col] = 'first'
    df_grouped = df.groupby('subscription_id').agg(agg_logic).reset_index()
    
    df_grouped['order_date'] = pd.to_datetime(df_grouped['order_date'], errors='coerce')
    df_grouped['ECHEANCE_date'] = pd.to_datetime(df_grouped['ECHEANCE_date'], errors='coerce')

    # Reconstruction
    to_reconstruct_order = pd.isna(df_grouped['order_date'])
    if to_reconstruct_order.any():
        date_cols = {'year': 'order_date (Année)', 'month': 'order_date (Mois)', 'day': 'order_date (Jour du mois)'}
        rename_dict = {v: k for k, v in date_cols.items()}
        df_rebuild = df_grouped.loc[to_reconstruct_order, list(date_cols.values())].rename(columns=rename_dict)
        df_grouped.loc[to_reconstruct_order, 'order_date'] = pd.to_datetime(df_rebuild, errors='coerce')

    to_reconstruct_echeance = pd.isna(df_grouped['ECHEANCE_date'])
    if to_reconstruct_echeance.any():
        date_cols = {'year': 'ECHEANCE_annee', 'month': 'ECHEANCE_mois', 'day': 'ECHEANCE_jour'}
        rename_dict = {v: k for k, v in date_cols.items()}
        df_rebuild = df_grouped.loc[to_reconstruct_echeance, list(date_cols.values())].rename(columns=rename_dict)
        df_grouped.loc[to_reconstruct_echeance, 'ECHEANCE_date'] = pd.to_datetime(df_rebuild, errors='coerce')
    
    # Imputation
    condition_fix_echeance = (df_grouped['frequence'].str.lower().str.contains('monthly', na=False) & pd.isna(df_grouped['ECHEANCE_date']) & pd.notna(df_grouped['order_date']))
    df_grouped.loc[condition_fix_echeance, 'ECHEANCE_date'] = df_grouped.loc[condition_fix_echeance, 'order_date'] + pd.Timedelta(days=30)
    
    df_grouped.dropna(subset=['order_date', 'ECHEANCE_date'], inplace=True)
    return df_grouped

def create_monthly_report(df_sub):
    """Transforme les abonnements en un rapport mensuel détaillé."""
    if df_sub is None: return None
    print("\n--- Étape 3: Création des parcours et expansion mensuelle ---")
    df_sorted = df_sub.sort_values(by=['customer_id', 'order_date']).copy()
    
    df_sorted['echeance_precedente'] = df_sorted.groupby('customer_id')['ECHEANCE_date'].shift(1)
    df_sorted['duree_trou_jours'] = (df_sorted['order_date'] - df_sorted['echeance_precedente']).dt.days
    seuil_churn_jours = np.where(df_sorted['frequence'].str.lower().str.contains('monthly', na=False), 35, 90)
    df_sorted['nouveau_parcours'] = np.where((df_sorted['duree_trou_jours'].isna()) | (df_sorted['duree_trou_jours'] >= seuil_churn_jours), True, False)
    
    df_sorted['id_parcours'] = df_sorted['customer_id'].astype(str) + '_' + df_sorted['nouveau_parcours'].cumsum().astype(str)
    df_sorted['journey_start_date'] = df_sorted.groupby('id_parcours')['order_date'].transform('min')
    
    all_months_data = []
    for row in df_sorted.itertuples(index=False):
        start_month = row.order_date.to_period('M').to_timestamp()
        end_month = row.ECHEANCE_date.to_period('M').to_timestamp()
        date_range = pd.date_range(start=start_month, end=end_month, freq='MS')
        for month_date in date_range:
            month_relatif = (month_date.year - row.journey_start_date.year) * 12 + (month_date.month - row.journey_start_date.month)
            new_row = {col: getattr(row, col) for col in row._fields}
            new_row.update({'month': month_date, 'month_relatif': month_relatif})
            all_months_data.append(new_row)
    return pd.DataFrame(all_months_data)


# --- PARTIE 2 : FONCTIONS D'ANALYSE ---

def calculate_retention_tables(df):
    """Calcule la rétention globale et segmentée et retourne les tables de résultats."""
    if df is None or df.empty: return {}
    print("\n--- Étape 4: Calcul des tables de rétention ---")
    
    results = {}
    
    # Rétention Globale
    initial_customers_global = df[df['month_relatif'] == 0]['customer_id'].nunique()
    if initial_customers_global > 0:
        retained_global = df.groupby('month_relatif')['customer_id'].nunique()
        retention_global_rate = (retained_global / initial_customers_global)
        results['Retention_Globale'] = retention_global_rate.to_frame(name='Taux_Retention')

    # Rétention Segmentée
    df_initial_state = df[df['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    characteristics = ['nom_offre', 'frequence', 'psp', 'payment_origin']
    for char in characteristics:
        top_segments = df_initial_state[char].value_counts().nlargest(7).index
        retention_by_segment = {}
        for segment in top_segments:
            cohort_ids = df_initial_state[df_initial_state[char] == segment]['customer_id'].unique()
            initial_count = len(cohort_ids)
            if initial_count == 0: continue
            df_cohort = df[df['customer_id'].isin(cohort_ids)]
            retained_count = df_cohort.groupby('month_relatif')['customer_id'].nunique()
            retention_rate = (retained_count / initial_count)
            retention_by_segment[segment] = retention_rate
        results[f'Retention_par_{char}'] = pd.DataFrame(retention_by_segment).fillna(0)
        
    return results

def characterize_cohorts(df):
    """Analyse et compare les profils des churners précoces, standards et super fidèles."""
    if df is None or df.empty: return {}
    print("\n--- Étape 5: Analyse des profils de cohortes ---")
    
    results = {}
    df['duree_parcours'] = df.groupby('id_parcours')['month_relatif'].transform('max')
    
    # Définition des cohortes
    df_early_churn = df[df['duree_parcours'] <= 2].copy()
    df_standard = df[(df['duree_parcours'] > 2) & (df['duree_parcours'] <= 12)].copy()
    df_super_loyal = df[df['duree_parcours'] > 12].copy()

    # Isoler l'état initial de chaque parcours pour chaque cohorte
    churn_initial = df_early_churn[df_early_churn['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    standard_initial = df_standard[df_standard['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    loyal_initial = df_super_loyal[df_super_loyal['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    
    print(f"Taille des cohortes (parcours uniques) : Churners Précoces({len(churn_initial)}), Standards({len(standard_initial)}), Super Fidèles({len(loyal_initial)})")

    characteristics = ['nom_offre', 'frequence', 'payment_origin', 'psp', 'tm_source']
    for char in characteristics:
        churn_dist = churn_initial[char].value_counts(normalize=True).mul(100)
        standard_dist = standard_initial[char].value_counts(normalize=True).mul(100)
        loyal_dist = loyal_initial[char].value_counts(normalize=True).mul(100)
        
        df_dist = pd.DataFrame({
            'Churners_Precoces (%)': churn_dist,
            'Standards (3-12m) (%)': standard_dist,
            'Super_Fideles (>12m) (%)': loyal_dist,
        }).fillna(0).sort_values(by='Super_Fideles (>12m) (%)', ascending=False)
        results[f'Profil_par_{char}'] = df_dist.head(15).round(2)
        
    return results

# --- PARTIE 3 : EXÉCUTION DU PIPELINE ---

def main():
    """Fonction principale qui orchestre l'ensemble du pipeline."""
    df_initial = load_and_merge_data(TRANSACTIONS_FILE, COUPONS_FILE)
    df_aggregated = group_and_repair_data(df_initial)
    df_monthly_report = create_monthly_report(df_aggregated)
    
    if df_monthly_report is not None:
        retention_results = calculate_retention_tables(df_monthly_report)
        cohort_results = characterize_cohorts(df_monthly_report)
        
        # Sauvegarde de tous les résultats dans un unique fichier Excel
        print(f"\n--- Étape 6: Sauvegarde du rapport Excel complet ---")
        try:
            with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
                for sheet_name, df_result in retention_results.items():
                    df_result.to_excel(writer, sheet_name=sheet_name)
                for sheet_name, df_result in cohort_results.items():
                    df_result.to_excel(writer, sheet_name=sheet_name)
            print(f"-> Pipeline terminé. Fichier de sortie : '{OUTPUT_FILE}'")
        except Exception as e:
            print(f"ERREUR CRITIQUE lors de la sauvegarde du fichier Excel : {e}")

if __name__ == "__main__":
    main()