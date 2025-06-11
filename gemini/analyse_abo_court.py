# -*- coding: utf-8 -*-
"""
Pipeline complet et final de traitement et d'analyse des données d'abonnements.
Version définitive calibrée sur les données réelles ('monthly', 'annual').
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

# --- Définition des fonctions ---

def load_and_merge_data(transactions_path, coupons_path):
    """Charge, fusionne, nettoie et enrichit les données initiales."""
    print("--- Étape 1: Chargement, fusion et enrichissement ---")
    try:
        df_trans = pd.read_csv(transactions_path, encoding='latin1')
        df_coupons = pd.read_csv(coupons_path, encoding='latin1')
        df_merged = pd.merge(df_trans, df_coupons, left_on='discount', right_on='Coupon Id', how='left')
        df_cleaned = df_merged.drop_duplicates().reset_index(drop=True)
        
        df_cleaned['nom_offre'] = np.where(pd.notna(df_cleaned['tm_campaign']), df_cleaned['tm_campaign'], df_cleaned['discount'])
        df_cleaned['nom_offre'] = df_cleaned['nom_offre'].fillna('Offre Standard')
        print("-> Fichiers chargés et 'nom_offre' créé.")
        return df_cleaned
    except Exception as e:
        print(f"ERREUR CRITIQUE lors du chargement : {e}")
        return None

def group_by_subscription(df):
    """Agrège les données pour avoir une ligne unique par subscription_id."""
    if df is None: return None
    print("\n--- Étape 2: Agrégation par subscription_id ---")
    agg_logic = {col: 'first' for col in df.columns if col != 'subscription_id'}
    agg_logic['ECHEANCE_date'] = 'last'
    df_grouped = df.groupby('subscription_id').agg(agg_logic).reset_index()
    return df_grouped

def create_monthly_report(df_sub):
    """Transforme les abonnements en un rapport mensuel détaillé."""
    if df_sub is None: return None
    print("\n--- Étape 3: Création des parcours et expansion mensuelle ---")
    
    df_sub['order_date'] = pd.to_datetime(df_sub['order_date'], errors='coerce')
    df_sub['ECHEANCE_date'] = pd.to_datetime(df_sub['ECHEANCE_date'], errors='coerce')
    df_sorted = df_sub.sort_values(by=['customer_id', 'order_date']).copy()
    
    # --- Logique du délai de grâce flexible ---
    df_sorted['echeance_precedente'] = df_sorted.groupby('customer_id')['ECHEANCE_date'].shift(1)
    df_sorted['duree_trou_jours'] = (df_sorted['order_date'] - df_sorted['echeance_precedente']).dt.days
    
    # CORRECTION : On cherche 'monthly' au lieu de 'mensuel'
    seuil_churn_jours = np.where(
        df_sorted['frequence'].str.lower().str.contains('monthly', na=False),
        35,  # Délai de grâce de 35 jours pour les mensuels
        90   # Délai de grâce de 90 jours pour les autres
    )
    
    df_sorted['nouveau_parcours'] = np.where(
        (df_sorted['duree_trou_jours'].isna()) | (df_sorted['duree_trou_jours'] >= seuil_churn_jours), 
        True, 
        False
    )
    
    df_sorted['id_parcours'] = df_sorted['customer_id'].astype(str) + '_' + df_sorted['nouveau_parcours'].cumsum().astype(str)
    df_sorted['journey_start_date'] = df_sorted.groupby('id_parcours')['order_date'].transform('min')
    print("-> Logique de parcours avec délai de grâce flexible appliquée.")

    all_months_data = []
    for row in df_sorted.itertuples(index=False):
        if pd.isna(row.order_date) or pd.isna(row.ECHEANCE_date): continue
        start_month = row.order_date.to_period('M').to_timestamp()
        end_month = row.ECHEANCE_date.to_period('M').to_timestamp()
        date_range = pd.date_range(start=start_month, end=end_month, freq='MS')

        for month_date in date_range:
            month_relatif = (month_date.year - row.journey_start_date.year) * 12 + (month_date.month - row.journey_start_date.month)
            montant_mensuel = 0
            try:
                revenue = float(str(getattr(row, 'consolidated_revenues_ht_euro', '0')).replace(',', '.'))
                frequence = str(getattr(row, 'frequence', '')).lower()
                # CORRECTION : On cherche 'annual' et 'monthly'
                if 'annual' in frequence: montant_mensuel = revenue / 12
                elif 'monthly' in frequence: montant_mensuel = revenue
            except (ValueError, TypeError): pass
            
            new_row = {col: getattr(row, col) for col in row._fields}
            new_row.update({'month': month_date, 'month_relatif': month_relatif, 'Montant': montant_mensuel})
            all_months_data.append(new_row)
            
    final_df = pd.DataFrame(all_months_data)
    print("-> Expansion mensuelle terminée.")
    return final_df

def analyze_churn_characteristics(df):
    """Identifie les clients churnés tôt et analyse leurs caractéristiques."""
    if df is None or df.empty: return
    print("\n--- Étape 4: Analyse des caractéristiques du Churn ---")
    
    df['duree_parcours'] = df.groupby('id_parcours')['month_relatif'].transform('max')
    
    df_churners = df[df['duree_parcours'] <= 2].copy()
    df_retained = df[df['duree_parcours'] > 2].copy()
    
    print(f"-> {df_churners['customer_id'].nunique()} clients ont churné dans les 3 premiers mois.")
    print(f"-> {df_retained['customer_id'].nunique()} clients sont restés au-delà de 3 mois.")

    churners_initial = df_churners[df_churners['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])
    retained_initial = df_retained[df_retained['month_relatif'] == 0].drop_duplicates(subset=['id_parcours'])

    caracteristiques_a_analyser = ['nom_offre', 'frequence', 'payment_origin', 'psp', 'tm_source', 'tm_medium', 'tm_campaign']
    
    for char in caracteristiques_a_analyser:
        print(f"\n--- Comparaison pour : {char} ---")
        churn_dist = churners_initial[char].value_counts(normalize=True).mul(100).rename('Churners (%)')
        retained_dist = retained_initial[char].value_counts(normalize=True).mul(100).rename('Retenus (%)')
        
        df_dist = pd.concat([churn_dist, retained_dist], axis=1).fillna(0).sort_values(by='Churners (%)', ascending=False)
        print(df_dist.head(10).round(2))
        
        df_plot_data = df_dist.head(5).reset_index().rename(columns={'index': char})
        df_plot = pd.melt(df_plot_data, id_vars=char, var_name='Groupe', value_name='Pourcentage')
        
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df_plot, x=char, y='Pourcentage', hue='Groupe', palette=['salmon', 'lightblue'])
        plt.title(f"Distribution de '{char}' pour les Churners vs Retenus", fontsize=16)
        plt.ylabel("Pourcentage de clients (%)", fontsize=12)
        plt.xlabel(char, fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

def main():
    """Fonction principale qui orchestre l'ensemble du pipeline."""
    df_initial = load_and_merge_data(TRANSACTIONS_FILE, COUPONS_FILE)
    df_aggregated = group_by_subscription(df_initial)
    df_monthly_report = create_monthly_report(df_aggregated)
    
    if df_monthly_report is not None:
        analyze_churn_characteristics(df_monthly_report)
        print(f"\n--- Sauvegarde du rapport final ---")
        try:
            df_monthly_report.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
            print(f"-> Pipeline terminé. Fichier de sortie : '{OUTPUT_FILE}'")
        except Exception as e:
            print(f"ERREUR CRITIQUE lors de la sauvegarde : {e}")

if __name__ == "__main__":
    main()