# -*- coding: utf-8 -*-
"""
Ce script identifie les clients dont le revenu a augmenté et fournit un
rapport détaillé comparant les caractéristiques de l'ancien et du nouvel abonnement.
"""
import pandas as pd
import numpy as np

# --- CONFIGURATION ---
TRANSACTIONS_FILE = 'transaction.csv'
COUPONS_FILE = 'Table des coupons-1.xlsx - Coupons.csv'
OUTPUT_FILE = 'rapport_upgrades_revenu_detaille.csv'

def load_and_prepare_data(transactions_path, coupons_path):
    """
    Charge, fusionne les données et les prépare pour l'analyse en
    créant une ligne unique et propre par abonnement.
    """
    print("--- Étape 1: Chargement et préparation des données ---")
    try:
        df_trans = pd.read_csv(transactions_path, encoding='latin1')
        df_coupons = pd.read_csv(coupons_path, encoding='latin1')
        df_merged = pd.merge(df_trans, df_coupons, left_on='discount', right_on='Coupon Id', how='left')
        df_cleaned = df_merged.drop_duplicates().reset_index(drop=True)
        
        df_cleaned['nom_offre'] = np.where(
            pd.notna(df_cleaned['tm_campaign']), 
            df_cleaned['tm_campaign'], 
            df_cleaned['discount']
        )
        df_cleaned['nom_offre'] = df_cleaned['nom_offre'].fillna('Offre Standard')

        # Agrégation pour avoir une ligne par abonnement. 'first' préserve toutes les colonnes.
        agg_logic = {col: 'first' for col in df_cleaned.columns if col != 'subscription_id'}
        agg_logic['order_date'] = 'first' # Garder la première date de transaction comme référence
        df_grouped = df_cleaned.groupby('subscription_id').agg(agg_logic).reset_index()
        
        # Nettoyage et conversion des types
        df_grouped['order_date'] = pd.to_datetime(df_grouped['order_date'], errors='coerce')
        df_grouped['consolidated_revenues_ht_euro'] = pd.to_numeric(
            df_grouped['consolidated_revenues_ht_euro'].astype(str).str.replace(',', '.'), 
            errors='coerce'
        )
        
        # Supprimer les lignes où les données essentielles pour l'analyse sont manquantes
        df_grouped.dropna(subset=['customer_id', 'order_date', 'consolidated_revenues_ht_euro'], inplace=True)
        
        print("-> Données chargées et préparées avec succès.")
        return df_grouped

    except Exception as e:
        print(f"ERREUR CRITIQUE lors du chargement : {e}")
        return None

def find_revenue_upgrades(df):
    """
    Identifie les augmentations de revenu et enrichit le DataFrame avec les données de l'abonnement précédent.
    """
    if df is None or df.empty:
        print("Le DataFrame est vide, analyse impossible.")
        return None
        
    print("\n--- Étape 2: Recherche des augmentations de revenu ---")
    
    df_sorted = df.sort_values(by=['customer_id', 'order_date'])
    
    # Définir toutes les colonnes à comparer
    cols_to_shift = [
        'consolidated_revenues_ht_euro', 'nom_offre', 'frequence', 
        'subscription_id', 'discount', 'psp', 'payment_origin',
        'tm_source', 'tm_medium', 'tm_campaign'
    ]
    
    # Créer les colonnes '_precedent' pour chaque caractéristique
    for col in cols_to_shift:
        # S'assurer que la colonne existe avant de faire le 'shift'
        if col in df_sorted.columns:
            df_sorted[f'{col}_precedent'] = df_sorted.groupby('customer_id')[col].shift(1)
        else:
            print(f"Avertissement : La colonne '{col}' est manquante et ne sera pas comparée.")
    
    # Isoler les lignes où le revenu actuel est supérieur au revenu précédent
    df_upgrades = df_sorted[
        (df_sorted['consolidated_revenues_ht_euro'] > df_sorted['consolidated_revenues_ht_euro_precedent'])
    ].copy()
    
    print(f"-> {len(df_upgrades)} abonnements correspondent à une augmentation de revenu.")
    
    return df_upgrades

def main():
    """
    Fonction principale qui orchestre l'ensemble du pipeline.
    """
    df_prepared = load_and_prepare_data(TRANSACTIONS_FILE, COUPONS_FILE)
    df_revenue_upgrades = find_revenue_upgrades(df_prepared)
    
    if df_revenue_upgrades is not None and not df_revenue_upgrades.empty:
        # Définir le dictionnaire des colonnes du rapport
        report_columns = {
            'customer_id': 'ID_Client',
            'order_date': 'Date_Upgrade',
            # Détails de l'ancien abonnement
            'subscription_id_precedent': 'ID_Ancien_Abo',
            'nom_offre_precedent': 'Ancienne_Offre',
            'frequence_precedente': 'Ancienne_Frequence',
            'consolidated_revenues_ht_euro_precedent': 'Ancien_Revenu_HT',
            'discount_precedent': 'Ancien_Discount',
            'psp_precedent': 'Ancien_PSP',
            'payment_origin_precedent': 'Ancienne_Origine',
            'tm_source_precedent': 'Ancienne_Source_TM',
            'tm_medium_precedent': 'Ancien_Medium_TM',
            'tm_campaign_precedent': 'Ancienne_Campagne_TM',
            # Détails du nouvel abonnement (l'upgrade)
            'subscription_id': 'ID_Nouvel_Abo',
            'nom_offre': 'Nouvelle_Offre',
            'frequence': 'Nouvelle_Frequence',
            'consolidated_revenues_ht_euro': 'Nouveau_Revenu_HT',
            'discount': 'Nouveau_Discount',
            'psp': 'Nouveau_PSP',
            'payment_origin': 'Nouvelle_Origine',
            'tm_source': 'Nouvelle_Source_TM',
            'tm_medium': 'Nouveau_Medium_TM',
            'tm_campaign': 'Nouvelle_Campagne_TM',
        }
        
        # On ne garde que les colonnes qui existent réellement dans le df après le 'shift'
        final_columns_to_select = [col for col in report_columns.keys() if col in df_revenue_upgrades.columns]
        
        final_report_df = df_revenue_upgrades[final_columns_to_select].rename(columns=report_columns)
        
        # Sauvegarde du rapport
        print(f"\n--- Étape 3: Sauvegarde du rapport très détaillé ---")
        try:
            final_report_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
            print(f"-> Pipeline terminé. Le rapport a été sauvegardé dans : '{OUTPUT_FILE}'")
            print("\nExtrait du rapport des augmentations de revenu :\n")
            print(final_report_df.head())
        except Exception as e:
            print(f"ERREUR CRITIQUE lors de la sauvegarde : {e}")
    else:
        print("\nAucune augmentation de revenu n'a été trouvée dans le jeu de données.")

# Point d'entrée du script
if __name__ == "__main__":
    main()
    