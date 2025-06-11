# -*- coding: utf-8 -*-
"""
Pipeline complet et final de traitement et d'analyse des données d'abonnements.
Version ultime avec traçabilité de l'imputation des dates.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter

# --- CONFIGURATION ---
TRANSACTIONS_FILE = 'transaction.csv'
COUPONS_FILE = 'Table des coupons-1.xlsx - Coupons.csv'
OUTPUT_FILE = 'rapport_mensuel_detaille.csv'

def load_and_merge_data(transactions_path, coupons_path):
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
    if df is None: return None
    print("\n--- Étape 2: Agrégation et réparation des dates ---")
    
    agg_logic = {col: 'first' for col in df.columns if col != 'subscription_id'}
    agg_logic['order_date'] = 'first'
    agg_logic['ECHEANCE_date'] = 'last'
    for col in ['order_date (Année)', 'order_date (Mois)', 'order_date (Jour du mois)', 'ECHEANCE_annee', 'ECHEANCE_mois', 'ECHEANCE_jour']:
        agg_logic[col] = 'first'
    df_grouped = df.groupby('subscription_id').agg(agg_logic).reset_index()
    
    print("-> Début de la réparation des dates en 3 phases...")
    df_grouped['methode_reparation_date'] = 'Originale' # Par défaut, la date est considérée comme correcte
    
    df_grouped['order_date_dt'] = pd.to_datetime(df_grouped['order_date'], errors='coerce')
    df_grouped['ECHEANCE_date_dt'] = pd.to_datetime(df_grouped['ECHEANCE_date'], errors='coerce')

    # Phase 2: Reconstruction
    to_reconstruct_order = pd.isna(df_grouped['order_date_dt'])
    if to_reconstruct_order.any():
        date_cols = {'year': 'order_date (Année)', 'month': 'order_date (Mois)', 'day': 'order_date (Jour du mois)'}
        rename_dict = {v: k for k, v in date_cols.items()}
        df_rebuild = df_grouped.loc[to_reconstruct_order, list(date_cols.values())].rename(columns=rename_dict)
        reconstructed_dates = pd.to_datetime(df_rebuild, errors='coerce')
        df_grouped.loc[to_reconstruct_order, 'order_date_dt'] = reconstructed_dates
        df_grouped.loc[reconstructed_dates.notna().index, 'methode_reparation_date'] = 'Reconstruite'

    to_reconstruct_echeance = pd.isna(df_grouped['ECHEANCE_date_dt'])
    if to_reconstruct_echeance.any():
        date_cols = {'year': 'ECHEANCE_annee', 'month': 'ECHEANCE_mois', 'day': 'ECHEANCE_jour'}
        rename_dict = {v: k for k, v in date_cols.items()}
        df_rebuild = df_grouped.loc[to_reconstruct_echeance, list(date_cols.values())].rename(columns=rename_dict)
        reconstructed_dates = pd.to_datetime(df_rebuild, errors='coerce')
        df_grouped.loc[to_reconstruct_echeance, 'ECHEANCE_date_dt'] = reconstructed_dates
        df_grouped.loc[reconstructed_dates.notna().index, 'methode_reparation_date'] = 'Reconstruite'
        
    # Phase 3: Imputation
    condition_fix_echeance = (df_grouped['frequence'].str.lower().str.contains('monthly', na=False) & pd.isna(df_grouped['ECHEANCE_date_dt']) & pd.notna(df_grouped['order_date_dt']))
    df_grouped.loc[condition_fix_echeance, 'ECHEANCE_date_dt'] = df_grouped.loc[condition_fix_echeance, 'order_date_dt'] + pd.Timedelta(days=30)
    df_grouped.loc[condition_fix_echeance, 'methode_reparation_date'] = 'Imputée (+30j)'

    df_grouped['order_date'] = df_grouped['order_date_dt']
    df_grouped['ECHEANCE_date'] = df_grouped['ECHEANCE_date_dt']
    
    initial_rows = len(df_grouped)
    df_grouped.dropna(subset=['order_date', 'ECHEANCE_date'], inplace=True)
    print(f"-> {initial_rows - len(df_grouped)} abonnements inutilisables restants ont été supprimés.")
    return df_grouped

def create_monthly_report(df_sub):
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
            montant_mensuel = 0
            try:
                revenue = float(str(getattr(row, 'consolidated_revenues_ht_euro', '0')).replace(',', '.'))
                frequence = str(getattr(row, 'frequence', '')).lower()
                if 'annual' in frequence: montant_mensuel = revenue / 12
                elif 'monthly' in frequence: montant_mensuel = revenue
            except (ValueError, TypeError): pass
            new_row = {col: getattr(row, col) for col in row._fields}
            new_row.update({'month': month_date, 'month_relatif': month_relatif, 'Montant': montant_mensuel})
            all_months_data.append(new_row)
    return pd.DataFrame(all_months_data)

def analyze_churn_characteristics(df):
    if df is None or df.empty: return
    print("\n--- Étape 4: Analyse des caractéristiques du Churn ---")
    df['duree_parcours'] = df.groupby('id_parcours')['month_relatif'].transform('max')
    df_churners = df[df['duree_parcours'] <= 2].copy()
    df_retained = df[df['duree_parcours'] > 2].copy()
    print(f"-> {df_churners['customer_id'].nunique()} clients ont churné dans les 3 premiers mois.")
    print(f"-> {df_retained['customer_id'].nunique()} clients sont restés au-delà de 3 mois.")
    churners_initial = df_churners[df_churners['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    retained_initial = df_retained[df_retained['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    caracteristiques_a_analyser = ['nom_offre', 'frequence', 'payment_origin', 'psp']
    for char in caracteristiques_a_analyser:
        print(f"\n--- Comparaison pour : {char} ---")
        churn_dist = churners_initial[char].value_counts(normalize=True).mul(100).rename('Churners (%)')
        retained_dist = retained_initial[char].value_counts(normalize=True).mul(100).rename('Retenus (%)')
        df_dist = pd.concat([churn_dist, retained_dist], axis=1).fillna(0).sort_values(by='Churners (%)', ascending=False)
        print(df_dist.head(10).round(2))

def main():
    """Fonction principale qui orchestre l'ensemble du pipeline."""
    df_initial = load_and_merge_data(TRANSACTIONS_FILE, COUPONS_FILE)
    df_aggregated = group_and_repair_data(df_initial)
    df_monthly_report = create_monthly_report(df_aggregated)
    if df_monthly_report is not None:
        analyze_churn_characteristics(df_monthly_report)
        
        # --- NOUVELLE SECTION : ANALYSE DE L'IMPUTATION ---
        print("\n" + "="*50)
        print("--- ANALYSE DE LA RÉPARATION DES DONNÉES ---")
        print("="*50)
        # On ne compte qu'une fois par parcours pour avoir le résumé par abonnement
        # On se base sur le df agrégé et réparé pour ce décompte
        print("Répartition des méthodes de réparation des dates sur les abonnements uniques :")
        print(df_aggregated['methode_reparation_date'].value_counts())
        
        print(f"\n--- Sauvegarde du rapport final ---")
        try:
            # On ajoute la colonne de méthode de réparation au rapport final pour analyse future
            df_to_save = df_monthly_report.merge(
                df_aggregated[['subscription_id', 'methode_reparation_date']],
                on='subscription_id',
                how='left'
            )
            df_to_save.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
            print(f"-> Pipeline terminé. Fichier de sortie : '{OUTPUT_FILE}'")
        except Exception as e:
            print(f"ERREUR CRITIQUE lors de la sauvegarde : {e}")

if __name__ == "__main__":
    main()