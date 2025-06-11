# -*- coding: utf-8 -*-
"""
Pipeline de traitement des données d'abonnements.

Ce script réalise les opérations suivantes :
1. Charge et fusionne les données de transactions et de coupons.
2. Nettoie les données fusionnées.
3. Agrège les données par abonnement (subscription_id) pour créer une vue unique.
4. Identifie les "parcours" clients en liant les réabonnements (écarts < 90 jours).
5. "Explose" chaque parcours en lignes mensuelles détaillées.
6. Calcule des métriques clés comme le revenu mensuel ('Montant') et le mois relatif.
7. Sauvegarde le résultat dans un fichier CSV.
"""
import pandas as pd
import numpy as np

# --- CONFIGURATION ---
# Modifiez ces chemins si vos fichiers sont dans un autre dossier.
TRANSACTIONS_FILE = 'transaction.csv'
COUPONS_FILE = 'Table des coupons-1.xlsx - Coupons.csv'
OUTPUT_FILE = 'rapport_mensuel_detaille.csv'


def load_and_merge_data(transactions_path, coupons_path):
    """
    Charge les fichiers CSV, les fusionne et effectue un premier nettoyage.
    """
    print("--- Étape 1: Chargement et fusion des données sources ---")
    try:
        df_trans = pd.read_csv(transactions_path, encoding='latin1')
        df_coupons = pd.read_csv(coupons_path, encoding='latin1')

        # Fusion en utilisant le nom de colonne corrigé 'Coupon Id'
        # Utilise une jointure à gauche pour conserver toutes les transactions.
        df_merged = pd.merge(df_trans, df_coupons, left_on='discount', right_on='Coupon Id', how='left')
        
        # Suppression des doublons exacts
        df_cleaned = df_merged.drop_duplicates().reset_index(drop=True)
        print(f"-> Fichiers chargés et fusionnés. Résultat : {df_cleaned.shape[0]} lignes.")
        return df_cleaned

    except FileNotFoundError as e:
        print(f"ERREUR CRITIQUE : Fichier source introuvable : {e}")
        return None
    except KeyError as e:
        print(f"ERREUR CRITIQUE DE CLÉ : La colonne de jointure {e} est introuvable.")
        print("Veuillez vérifier les noms de colonnes dans vos fichiers CSV.")
        return None


def group_by_subscription(df):
    """
    Agrège les données pour avoir une ligne unique et propre par subscription_id.
    """
    if df is None:
        return None
    
    print("\n--- Étape 2: Agrégation par subscription_id ---")
    
    # Définition d'une logique d'agrégation intelligente
    # 'first' pour les données stables, 'last' pour les dates de fin,
    # et concaténation pour les données qui peuvent varier.
    agg_logic = {col: 'first' for col in df.columns}
    agg_logic['ECHEANCE_date'] = 'last'
    
    # On retire la clé de groupe de la logique d'agrégation
    agg_logic.pop('subscription_id', None)

    df_grouped = df.groupby('subscription_id').agg(agg_logic).reset_index()
    print(f"-> Agrégation terminée. Résultat : {df_grouped.shape[0]} lignes.")
    return df_grouped


def create_monthly_report(df_sub):
    """
    Transforme les données d'abonnement en un rapport mensuel détaillé.
    """
    if df_sub is None:
        return None
        
    print("\n--- Étape 3: Création des parcours et expansion mensuelle ---")
    
    # 3.1. Préparation et identification des parcours
    df_sub['order_date'] = pd.to_datetime(df_sub['order_date'], errors='coerce')
    df_sub['ECHEANCE_date'] = pd.to_datetime(df_sub['ECHEANCE_date'], errors='coerce')
    df_sorted = df_sub.sort_values(by=['customer_id', 'order_date']).copy()

    df_sorted['echeance_precedente'] = df_sorted.groupby('customer_id')['ECHEANCE_date'].shift(1)
    df_sorted['duree_trou_jours'] = (df_sorted['order_date'] - df_sorted['echeance_precedente']).dt.days
    df_sorted['nouveau_parcours'] = np.where(
        (df_sorted['duree_trou_jours'].isna()) | (df_sorted['duree_trou_jours'] >= 90), True, False
    )
    df_sorted['parcours_numero'] = df_sorted.groupby('customer_id')['nouveau_parcours'].cumsum()
    print("-> Logique de parcours appliquée.")

    # 3.2. Expansion de chaque abonnement en lignes mensuelles
    all_months_data = []
    for row in df_sorted.itertuples(index=False):
        if pd.isna(row.order_date) or pd.isna(row.ECHEANCE_date):
            continue

        date_range = pd.date_range(start=row.order_date, end=row.ECHEANCE_date, freq='MS')
        if date_range.empty and pd.notna(row.order_date):
            date_range = pd.to_datetime([row.order_date]).to_period('M').to_timestamp()

        for month_date in date_range:
            month_relatif = (month_date.year - row.order_date.year) * 12 + (month_date.month - row.order_date.month)
            
            montant_mensuel = 0
            try:
                revenue_str = str(getattr(row, 'consolidated_revenues_ht_euro', '0')).replace(',', '.')
                revenue = float(revenue_str)
                frequence_str = str(getattr(row, 'frequence', '')).lower()
                if 'annuel' in frequence_str:
                    montant_mensuel = revenue / 12
                elif 'mensuel' in frequence_str:
                    montant_mensuel = revenue
            except (ValueError, TypeError):
                montant_mensuel = 0

            new_row = {
                'offer_date': row.order_date, 'month': month_date, 'enrollment_month_number': row.order_date.month,
                'month_relatif': month_relatif, 'is_first_subscription': row.parcours_numero == 1,
                'Montant': montant_mensuel, 'customer_id': row.customer_id, 'subscription_id': row.subscription_id,
                'frequence': row.frequence,
                'order_date (Année)': row.order_date.year, 'order_date (Mois)': row.order_date.month,
                'order_date (Jour du mois)': row.order_date.day, 'order_date': row.order_date,
                'payment_origin': row.payment_origin, 'ECHEANCE_annee': row.ECHEANCE_date.year,
                'ECHEANCE_mois': row.ECHEANCE_date.month, 'ECHEANCE_jour': row.ECHEANCE_date.day,
                'date_fin_abo': row.ECHEANCE_date, 'psp': row.psp, 'order_paid_date_processed': row.order_paid_date_processed,
                'discount': row.discount, 'custom_discount': row.custom_discount, 'tm_source': row.tm_source,
                'tm_medium': row.tm_medium, 'tm_campaign': row.tm_campaign,
                'consolidated_revenues_ht_euro': row.consolidated_revenues_ht_euro
            }
            all_months_data.append(new_row)
    
    final_df = pd.DataFrame(all_months_data)
    print(f"-> Expansion mensuelle terminée. Résultat : {final_df.shape[0]} lignes.")
    return final_df

def main():
    """
    Fonction principale qui orchestre l'ensemble du pipeline.
    """
    # Étape 1
    df_initial = load_and_merge_data(TRANSACTIONS_FILE, COUPONS_FILE)
    
    # Étape 2
    df_aggregated = group_by_subscription(df_initial)
    
    # Étape 3
    df_monthly_report = create_monthly_report(df_aggregated)
    
    if df_monthly_report is not None:
        # Étape 4 : Sauvegarde
        print(f"\n--- Étape 4: Sauvegarde du rapport final ---")
        try:
            # Réordonner les colonnes pour la lisibilité
            final_columns = [
                'customer_id', 'subscription_id', 'month', 'Montant', 'month_relatif', 
                'is_first_subscription', 'offer_date', 'date_fin_abo', 'frequence',
                'enrollment_month_number', 'consolidated_revenues_ht_euro',
                'order_date', 'ECHEANCE_annee', 'ECHEANCE_mois', 'ECHEANCE_jour', 'psp'
            ]
            # Ajouter les colonnes restantes qui existent
            for col in df_monthly_report.columns:
                if col not in final_columns:
                    final_columns.append(col)
            
            df_monthly_report = df_monthly_report[final_columns]
            
            df_monthly_report.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
            print(f"-> Pipeline terminé avec succès. Fichier de sortie : '{OUTPUT_FILE}'")
            print("\nAperçu des 5 premières lignes du résultat final :\n")
            print(df_monthly_report.head())
        except Exception as e:
            print(f"ERREUR CRITIQUE lors de la sauvegarde du fichier final : {e}")

# Point d'entrée du script
if __name__ == "__main__":
    main()