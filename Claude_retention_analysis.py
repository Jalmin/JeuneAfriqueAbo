#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyse complète de rétention des abonnés avec segmentation avancée
Ce script traite les données de transaction pour calculer la rétention par cohorte
et par segments (fréquence, source, médium, PSP, revenus)

Version finale avec analyse jusqu'à M25
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def load_and_clean_data(file_path):
    """Charge et nettoie les données de transaction"""
    print("=== CHARGEMENT ET NETTOYAGE DES DONNÉES ===")
    
    # Charger les données
    df = pd.read_csv(file_path, encoding='utf-8')
    print(f"✓ {len(df):,} transactions chargées")
    print(f"✓ {df['customer_id'].nunique():,} clients uniques")
    
    # Utiliser les noms exacts des colonnes de votre fichier
    try:
        # Convertir les dates avec les noms exacts
        df['order_date'] = pd.to_datetime(df[['order_date (Année)', 'order_date (Mois)', 'order_date (Jour du mois)']])
        df['echeance_date'] = pd.to_datetime(df[['ECHEANCE_annee', 'ECHEANCE_mois', 'ECHEANCE_jour']])
        print("✓ Conversion des dates réussie")
    except Exception as e:
        print(f"ERREUR lors de la conversion des dates: {e}")
        # Méthode alternative avec concaténation
        try:
            df['order_date'] = pd.to_datetime(
                df['order_date (Année)'].astype(str) + '-' + 
                df['order_date (Mois)'].astype(str) + '-' + 
                df['order_date (Jour du mois)'].astype(str)
            )
            df['echeance_date'] = pd.to_datetime(
                df['ECHEANCE_annee'].astype(str) + '-' + 
                df['ECHEANCE_mois'].astype(str) + '-' + 
                df['ECHEANCE_jour'].astype(str)
            )
            print("✓ Conversion alternative réussie")
        except Exception as e2:
            print(f"ERREUR finale: {e2}")
            # Dernière tentative avec gestion des valeurs manquantes
            try:
                # Nettoyer les données avant conversion
                df_clean = df.dropna(subset=['order_date (Année)', 'order_date (Mois)', 'order_date (Jour du mois)',
                                           'ECHEANCE_annee', 'ECHEANCE_mois', 'ECHEANCE_jour'])
                
                df_clean['order_date'] = pd.to_datetime(
                    df_clean['order_date (Année)'].astype(int).astype(str) + '-' + 
                    df_clean['order_date (Mois)'].astype(int).astype(str) + '-' + 
                    df_clean['order_date (Jour du mois)'].astype(int).astype(str)
                )
                df_clean['echeance_date'] = pd.to_datetime(
                    df_clean['ECHEANCE_annee'].astype(int).astype(str) + '-' + 
                    df_clean['ECHEANCE_mois'].astype(int).astype(str) + '-' + 
                    df_clean['ECHEANCE_jour'].astype(int).astype(str)
                )
                
                # Remplacer le dataframe original
                df = df_clean
                print("✓ Conversion avec nettoyage réussie")
            except Exception as e3:
                print(f"ERREUR finale après nettoyage: {e3}")
                return None
    
    # Nettoyer les données
    df = df.dropna(subset=['order_date', 'echeance_date', 'customer_id'])
    df = df.sort_values(['customer_id', 'order_date'])
    
    print(f"✓ Données nettoyées: {len(df):,} transactions valides")
    print(f"✓ Période: {df['order_date'].min().strftime('%m/%Y')} à {df['order_date'].max().strftime('%m/%Y')}")
    
    return df

def create_monthly_retention_table(df):
    """Crée une table avec une ligne par mois d'activité pour chaque abonnement"""
    print("\n=== CRÉATION DE LA TABLE MENSUELLE ===")
    
    monthly_data = []
    total_customers = df['customer_id'].nunique()
    
    # Traiter chaque client
    for idx, customer_id in enumerate(df['customer_id'].unique()):
        if idx % 1000 == 0:
            print(f"  Traitement: {idx + 1:,}/{total_customers:,} clients")
            
        customer_data = df[df['customer_id'] == customer_id].copy()
        
        # Date de début du parcours client
        journey_start = customer_data['order_date'].min()
        journey_start_month = journey_start.replace(day=1)
        
        # Traiter chaque transaction
        for _, transaction in customer_data.iterrows():
            # Générer tous les mois couverts par cette transaction
            start_month = transaction['order_date'].replace(day=1)
            end_month = transaction['echeance_date'].replace(day=1)
            
            current_month = start_month
            while current_month <= end_month:
                # Calculer le mois relatif
                months_diff = (current_month.year - journey_start_month.year) * 12 + \
                             (current_month.month - journey_start_month.month)
                
                monthly_data.append({
                    'customer_id': customer_id,
                    'subscription_id': transaction['subscription_id'],
                    'mois': current_month.strftime('%m/%Y'),
                    'mois_relatif': months_diff,
                    'date_debut_parcours': journey_start_month.strftime('%m/%Y'),
                    'frequence': transaction['frequence'],
                    'type': transaction['type'],
                    'payment_origin': transaction['payment_origin'],
                    'psp': transaction['psp'],
                    'tm_source': transaction['tm_source'],
                    'tm_medium': transaction['tm_medium'],
                    'tm_campaign': transaction['tm_campaign'],
                    'consolidated_revenues_ht_euro': transaction['consolidated_revenues_ht_euro']
                })
                
                # Passer au mois suivant
                if current_month.month == 12:
                    current_month = current_month.replace(year=current_month.year + 1, month=1)
                else:
                    current_month = current_month.replace(month=current_month.month + 1)
    
    monthly_df = pd.DataFrame(monthly_data)
    
    # Dédupliquer (un client ne peut être actif qu'une fois par mois)
    monthly_df = monthly_df.drop_duplicates(subset=['customer_id', 'mois'])
    
    print(f"✓ Table mensuelle créée: {len(monthly_df):,} lignes")
    print(f"✓ Clients uniques: {monthly_df['customer_id'].nunique():,}")
    print(f"✓ Cohortes: {monthly_df['date_debut_parcours'].nunique()}")
    
    return monthly_df

def calculate_cohort_retention(monthly_df):
    """Calcule les taux de rétention par cohorte"""
    print("\n=== CALCUL DE LA RÉTENTION PAR COHORTE ===")
    
    retention_results = []
    
    # Pour chaque cohorte
    for cohort in monthly_df['date_debut_parcours'].unique():
        cohort_data = monthly_df[monthly_df['date_debut_parcours'] == cohort]
        
        # Nombre de clients au mois 0
        initial_customers = cohort_data[cohort_data['mois_relatif'] == 0]['customer_id'].nunique()
        
        if initial_customers == 0:
            continue
        
        # Calculer la rétention pour chaque mois relatif
        for month_rel in sorted(cohort_data['mois_relatif'].unique()):
            active_customers = cohort_data[cohort_data['mois_relatif'] == month_rel]['customer_id'].nunique()
            retention_rate = (active_customers / initial_customers) * 100
            
            retention_results.append({
                'cohorte': cohort,
                'mois_relatif': month_rel,
                'clients_initiaux': initial_customers,
                'clients_actifs': active_customers,
                'taux_retention': round(retention_rate, 2)
            })
    
    retention_df = pd.DataFrame(retention_results)
    retention_df = retention_df.sort_values(['cohorte', 'mois_relatif'])
    
    print(f"✓ Analyse terminée: {len(retention_df):,} points de données")
    
    return retention_df

def calculate_segmented_retention(monthly_df):
    """Calcule la rétention segmentée par différents critères"""
    print("\n=== ANALYSE DE RÉTENTION SEGMENTÉE ===")
    
    # Nettoyer et préparer les segments
    monthly_df_clean = monthly_df.copy()
    
    # Nettoyer le PSP (null = CB)
    monthly_df_clean['psp_clean'] = monthly_df_clean['psp'].fillna('CB')
    
    # Nettoyer les revenus (grouper par tranches)
    monthly_df_clean['revenue_tranche'] = pd.cut(
        monthly_df_clean['consolidated_revenues_ht_euro'].fillna(0),
        bins=[0, 5, 10, 15, 20, float('inf')],
        labels=['0-5€', '5-10€', '10-15€', '15-20€', '>20€']
    )
    
    segments = {
        'frequence': 'frequence',
        'tm_source': 'tm_source', 
        'tm_medium': 'tm_medium',
        'psp': 'psp_clean',
        'revenue_tranche': 'revenue_tranche'
    }
    
    segmented_results = {}
    
    for segment_name, column in segments.items():
        print(f"\nAnalyse par {segment_name}...")
        segment_retention = []
        
        # Pour chaque valeur du segment
        for segment_value in monthly_df_clean[column].dropna().unique():
            if pd.isna(segment_value):
                continue
                
            segment_data = monthly_df_clean[monthly_df_clean[column] == segment_value]
            
            # Pour chaque cohorte dans ce segment
            for cohort in segment_data['date_debut_parcours'].unique():
                cohort_segment_data = segment_data[segment_data['date_debut_parcours'] == cohort]
                
                # Clients initiaux (mois 0)
                initial_customers = cohort_segment_data[cohort_segment_data['mois_relatif'] == 0]['customer_id'].nunique()
                
                if initial_customers < 10:  # Seuil minimum pour la significativité
                    continue
                
                # Calculer la rétention pour chaque mois relatif
                for month_rel in sorted(cohort_segment_data['mois_relatif'].unique()):
                    active_customers = cohort_segment_data[cohort_segment_data['mois_relatif'] == month_rel]['customer_id'].nunique()
                    retention_rate = (active_customers / initial_customers) * 100
                    
                    segment_retention.append({
                        'segment_type': segment_name,
                        'segment_value': segment_value,
                        'cohorte': cohort,
                        'mois_relatif': month_rel,
                        'clients_initiaux': initial_customers,
                        'clients_actifs': active_customers,
                        'taux_retention': round(retention_rate, 2)
                    })
        
        if segment_retention:
            segmented_results[segment_name] = pd.DataFrame(segment_retention)
            
            # Afficher un résumé pour ce segment
            summary = segmented_results[segment_name].groupby(['segment_value', 'mois_relatif']).agg({
                'taux_retention': 'mean',
                'clients_initiaux': 'sum'
            }).round(2)
            
            print(f"  {len(segmented_results[segment_name])} points de données créés")
            
            # Afficher les performances par segment pour les mois clés
            for month in [1, 6, 12, 24]:
                if month in summary.index.get_level_values('mois_relatif'):
                    month_data = summary.xs(month, level='mois_relatif').sort_values('taux_retention', ascending=False)
                    if not month_data.empty:
                        print(f"    Mois {month} - Meilleur: {month_data.index[0]} ({month_data.iloc[0]['taux_retention']:.1f}%)")
    
    return segmented_results

def analyze_retention_trends(retention_df):
    """Analyse les tendances de rétention globales"""
    print("\n=== ANALYSE DES TENDANCES GLOBALES ===")
    
    # Filtrer les cohortes significatives (≥50 clients)
    significant_cohorts = retention_df[retention_df['clients_initiaux'] >= 50]['cohorte'].unique()
    significant_data = retention_df[retention_df['cohorte'].isin(significant_cohorts)]
    
    print(f"Cohortes significatives (≥50 clients): {len(significant_cohorts)}")
    
    # Calculer les moyennes par mois relatif
    avg_retention = significant_data.groupby('mois_relatif').agg({
        'taux_retention': 'mean',
        'clients_initiaux': 'sum',
        'clients_actifs': 'sum'
    }).round(2)
    
    print("\nTaux de rétention moyenne par mois relatif:")
    print("Mois\tRétention\tClients Initiaux\tClients Actifs")
    print("-" * 55)
    
    for month in [0, 1, 2, 3, 6, 12, 13, 18, 24, 25]:
        if month in avg_retention.index:
            row = avg_retention.loc[month]
            print(f"{month}\t{row['taux_retention']:.1f}%\t\t{row['clients_initiaux']:,.0f}\t\t{row['clients_actifs']:,.0f}")
    
    return significant_data, avg_retention

def create_cohort_summary(retention_df):
    """Crée un résumé par cohorte avec les métriques clés"""
    print("\n=== RÉSUMÉ PAR COHORTE ===")
    
    cohort_summary = []
    
    for cohort in retention_df['cohorte'].unique():
        cohort_data = retention_df[retention_df['cohorte'] == cohort]
        initial_size = cohort_data['clients_initiaux'].iloc[0]
        
        # Extraire les taux de rétention pour différents mois
        retention_1m = cohort_data[cohort_data['mois_relatif'] == 1]['taux_retention'].values
        retention_3m = cohort_data[cohort_data['mois_relatif'] == 3]['taux_retention'].values
        retention_6m = cohort_data[cohort_data['mois_relatif'] == 6]['taux_retention'].values
        retention_12m = cohort_data[cohort_data['mois_relatif'] == 12]['taux_retention'].values
        retention_13m = cohort_data[cohort_data['mois_relatif'] == 13]['taux_retention'].values
        retention_18m = cohort_data[cohort_data['mois_relatif'] == 18]['taux_retention'].values
        retention_24m = cohort_data[cohort_data['mois_relatif'] == 24]['taux_retention'].values
        retention_25m = cohort_data[cohort_data['mois_relatif'] == 25]['taux_retention'].values
        
        cohort_summary.append({
            'cohorte': cohort,
            'taille_initiale': initial_size,
            'retention_1m': retention_1m[0] if len(retention_1m) > 0 else np.nan,
            'retention_3m': retention_3m[0] if len(retention_3m) > 0 else np.nan,
            'retention_6m': retention_6m[0] if len(retention_6m) > 0 else np.nan,
            'retention_12m': retention_12m[0] if len(retention_12m) > 0 else np.nan,
            'retention_13m': retention_13m[0] if len(retention_13m) > 0 else np.nan,
            'retention_18m': retention_18m[0] if len(retention_18m) > 0 else np.nan,
            'retention_24m': retention_24m[0] if len(retention_24m) > 0 else np.nan,
            'retention_25m': retention_25m[0] if len(retention_25m) > 0 else np.nan,
            'duree_suivi': cohort_data['mois_relatif'].max()
        })
    
    summary_df = pd.DataFrame(cohort_summary)
    summary_df = summary_df.sort_values('cohorte')
    
    # Afficher les principales cohortes
    print("Principales cohortes (≥50 clients):")
    print("Cohorte\t\tTaille\t1M\t3M\t6M\t12M\t13M\t18M\t24M\t25M\tSuivi")
    print("-" * 85)
    
    large_cohorts = summary_df[summary_df['taille_initiale'] >= 50].head(15)
    for _, row in large_cohorts.iterrows():
        print(f"{row['cohorte']}\t{row['taille_initiale']:.0f}\t"
              f"{row['retention_1m']:.0f}%\t"
              f"{row['retention_3m']:.0f}%\t"
              f"{row['retention_6m']:.0f}%\t"
              f"{row['retention_12m']:.0f}%\t"
              f"{row['retention_13m']:.0f}%\t"
              f"{row['retention_18m']:.0f}%\t"
              f"{row['retention_24m']:.0f}%\t"
              f"{row['retention_25m']:.0f}%\t"
              f"{row['duree_suivi']:.0f}m")
    
    return summary_df

def create_segment_summary(segmented_results):
    """Crée un résumé des performances par segment"""
    print("\n=== RÉSUMÉ DES PERFORMANCES PAR SEGMENT ===")
    
    segment_summaries = {}
    
    for segment_name, segment_df in segmented_results.items():
        print(f"\n{segment_name.upper()}:")
        print("-" * 40)
        
        # Calculer les moyennes par segment pour les mois clés
        summary_data = []
        
        for segment_value in segment_df['segment_value'].unique():
            segment_subset = segment_df[segment_df['segment_value'] == segment_value]
            
            # Taille totale du segment
            total_clients = segment_subset[segment_subset['mois_relatif'] == 0]['clients_initiaux'].sum()
            
            if total_clients < 50:  # Filtrer les segments trop petits
                continue
            
            # Moyennes de rétention pour les mois clés
            retention_1m = segment_subset[segment_subset['mois_relatif'] == 1]['taux_retention'].mean()
            retention_3m = segment_subset[segment_subset['mois_relatif'] == 3]['taux_retention'].mean()
            retention_6m = segment_subset[segment_subset['mois_relatif'] == 6]['taux_retention'].mean()
            retention_12m = segment_subset[segment_subset['mois_relatif'] == 12]['taux_retention'].mean()
            retention_13m = segment_subset[segment_subset['mois_relatif'] == 13]['taux_retention'].mean()
            retention_18m = segment_subset[segment_subset['mois_relatif'] == 18]['taux_retention'].mean()
            retention_24m = segment_subset[segment_subset['mois_relatif'] == 24]['taux_retention'].mean()
            retention_25m = segment_subset[segment_subset['mois_relatif'] == 25]['taux_retention'].mean()
            
            summary_data.append({
                'segment': str(segment_value)[:20],  # Tronquer pour l'affichage
                'total_clients': total_clients,
                'retention_1m': retention_1m,
                'retention_3m': retention_3m,
                'retention_6m': retention_6m,
                'retention_12m': retention_12m,
                'retention_13m': retention_13m,
                'retention_18m': retention_18m,
                'retention_24m': retention_24m,
                'retention_25m': retention_25m
            })
        
        # Trier par rétention 24M et afficher
        summary_data = sorted(summary_data, key=lambda x: x['retention_24m'] if not pd.isna(x['retention_24m']) else 0, reverse=True)
        
        print("Segment\t\t\tClients\t1M\t3M\t6M\t12M\t13M\t18M\t24M\t25M")
        print("-" * 95)
        
        for item in summary_data[:10]:  # Top 10
            print(f"{item['segment']:<20}\t{item['total_clients']:.0f}\t"
                  f"{item['retention_1m']:.0f}%\t"
                  f"{item['retention_3m']:.0f}%\t"
                  f"{item['retention_6m']:.0f}%\t"
                  f"{item['retention_12m']:.0f}%\t"
                  f"{item['retention_13m']:.0f}%\t"
                  f"{item['retention_18m']:.0f}%\t"
                  f"{item['retention_24m']:.0f}%\t"
                  f"{item['retention_25m']:.0f}%")
        
        segment_summaries[segment_name] = summary_data
    
    return segment_summaries

def export_results(monthly_df, retention_df, summary_df, segmented_results, segment_summaries, output_file='analyse_retention_segmentee.xlsx'):
    """Exporte tous les résultats dans un fichier Excel avec segmentation"""
    print(f"\n=== EXPORT VERS {output_file} ===")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 1. Table mensuelle (échantillon)
        monthly_df.head(10000).to_excel(writer, sheet_name='Table_Mensuelle', index=False)
        
        # 2. Table de rétention globale par cohorte
        retention_df.to_excel(writer, sheet_name='Retention_Globale', index=False)
        
        # 3. Résumé par cohorte
        summary_df.to_excel(writer, sheet_name='Resume_Cohortes', index=False)
        
        # 4. Moyennes de rétention globales
        avg_by_month = retention_df.groupby('mois_relatif').agg({
            'taux_retention': 'mean',
            'clients_initiaux': 'sum'
        }).round(2)
        avg_by_month.to_excel(writer, sheet_name='Moyennes_Globales')
        
        # 5. Résultats segmentés
        for segment_name, segment_df in segmented_results.items():
            sheet_name = f'Retention_{segment_name.capitalize()}'[:31]  # Limite Excel
            segment_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # 6. Résumés des segments
        for segment_name, summary_data in segment_summaries.items():
            if summary_data:
                sheet_name = f'Resume_{segment_name.capitalize()}'[:31]
                pd.DataFrame(summary_data).to_excel(writer, sheet_name=sheet_name, index=False)
        
        # 7. Analyse comparative des segments
        comparative_data = []
        for segment_name, summary_data in segment_summaries.items():
            for item in summary_data[:5]:  # Top 5 de chaque segment
                comparative_data.append({
                    'type_segment': segment_name,
                    'segment': item['segment'],
                    'total_clients': item['total_clients'],
                    'retention_1m': item['retention_1m'],
                    'retention_6m': item['retention_6m'],
                    'retention_12m': item['retention_12m'],
                    'retention_18m': item['retention_18m'],
                    'retention_24m': item['retention_24m']
                })
        
        if comparative_data:
            pd.DataFrame(comparative_data).to_excel(writer, sheet_name='Comparatif_Segments', index=False)
    
    print(f"✓ Résultats exportés vers {output_file}")
    print(f"  - {len(segmented_results)} analyses segmentées")
    print(f"  - {sum(len(s) for s in segment_summaries.values())} segments analysés")

def analyze_revenue_correlation(monthly_df):
    """Analyse la corrélation entre revenus et rétention"""
    print("\n=== ANALYSE REVENUS / RÉTENTION ===")
    
    # Créer des tranches de revenus plus détaillées
    monthly_df_revenue = monthly_df.copy()
    monthly_df_revenue['revenue_clean'] = monthly_df_revenue['consolidated_revenues_ht_euro'].fillna(0)
    
    # Statistiques des revenus
    revenue_stats = monthly_df_revenue['revenue_clean'].describe()
    print("Statistiques des revenus:")
    print(f"  Moyenne: {revenue_stats['mean']:.2f}€")
    print(f"  Médiane: {revenue_stats['50%']:.2f}€")
    print(f"  Écart-type: {revenue_stats['std']:.2f}€")
    
    # Analyse par tranche de revenus
    monthly_df_revenue['revenue_quartile'] = pd.qcut(
        monthly_df_revenue['revenue_clean'], 
        q=4, 
        labels=['Q1 (Bas)', 'Q2 (Moyen-)', 'Q3 (Moyen+)', 'Q4 (Élevé)']
    )
    
    print("\nRétention par quartile de revenus:")
    for quartile in ['Q1 (Bas)', 'Q2 (Moyen-)', 'Q3 (Moyen+)', 'Q4 (Élevé)']:
        quartile_data = monthly_df_revenue[monthly_df_revenue['revenue_quartile'] == quartile]
        clients_month_0 = quartile_data[quartile_data['mois_relatif'] == 0]['customer_id'].nunique()
        clients_month_12 = quartile_data[quartile_data['mois_relatif'] == 12]['customer_id'].nunique()
        
        if clients_month_0 > 0:
            retention_12m = (clients_month_12 / clients_month_0) * 100
            avg_revenue = quartile_data['revenue_clean'].mean()
            print(f"  {quartile}: {retention_12m:.1f}% rétention 12M (revenus moy: {avg_revenue:.2f}€)")
    
    return monthly_df_revenue

def main():
    """Fonction principale avec segmentation complète"""
    print("ANALYSE DE RÉTENTION SEGMENTÉE DES ABONNÉS")
    print("=" * 60)
    
    try:
        # Configuration
        input_file = 'transaction_sample_by_customer.csv'
        output_file = 'analyse_retention_segmentee.xlsx'
        
        # 1. Charger et nettoyer les données
        df = load_and_clean_data(input_file)
        
        # 2. Créer la table mensuelle
        monthly_df = create_monthly_retention_table(df)
        
        # 3. Calculer la rétention globale par cohorte
        retention_df = calculate_cohort_retention(monthly_df)
        
        # 4. Analyser les tendances globales
        significant_data, avg_retention = analyze_retention_trends(retention_df)
        
        # 5. Créer le résumé global par cohorte
        summary_df = create_cohort_summary(retention_df)
        
        # 6. Analyser la rétention segmentée
        segmented_results = calculate_segmented_retention(monthly_df)
        
        # 7. Créer les résumés par segment
        segment_summaries = create_segment_summary(segmented_results)
        
        # 8. Analyser la corrélation revenus/rétention
        monthly_df_revenue = analyze_revenue_correlation(monthly_df)
        
        # 9. Exporter tous les résultats
        export_results(monthly_df, retention_df, summary_df, segmented_results, segment_summaries, output_file)
        
        print("\n" + "=" * 60)
        print("ANALYSE SEGMENTÉE TERMINÉE AVEC SUCCÈS!")
        print("=" * 60)
        
        # Statistiques finales détaillées
        print(f"\nSTATISTIQUES FINALES:")
        print(f"- Clients analysés: {monthly_df['customer_id'].nunique():,}")
        print(f"- Cohortes identifiées: {retention_df['cohorte'].nunique()}")
        print(f"- Segments analysés: {len(segmented_results)}")
        print(f"- Période d'analyse: {monthly_df['mois'].min()} à {monthly_df['mois'].max()}")
        
        # Moyennes globales
        if len(significant_data) > 0:
            ret_1m = significant_data[significant_data['mois_relatif']==1]['taux_retention'].mean()
            ret_12m = significant_data[significant_data['mois_relatif']==12]['taux_retention'].mean()
            ret_24m = significant_data[significant_data['mois_relatif']==24]['taux_retention'].mean()
            print(f"- Rétention moyenne 1M: {ret_1m:.1f}%")
            print(f"- Rétention moyenne 12M: {ret_12m:.1f}%")
            if not pd.isna(ret_24m):
                print(f"- Rétention moyenne 24M: {ret_24m:.1f}%")
        
        # Meilleurs segments
        print(f"\nMEILLEURS SEGMENTS (rétention 24M):")
        for segment_name, summary_data in segment_summaries.items():
            if summary_data:
                best_segment = summary_data[0]  # Premier = meilleur
                retention_val = best_segment.get('retention_24m', best_segment.get('retention_12m', 0))
                print(f"- {segment_name}: {best_segment['segment']} ({retention_val:.1f}%)")
        
        return monthly_df, retention_df, summary_df, segmented_results, segment_summaries
        
    except Exception as e:
        print(f"ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None, None, None

# Instructions d'utilisation
if __name__ == "__main__":
    print(__doc__)
    print("\nCe script analyse la rétention par segments jusqu'à M25 :")
    print("• Fréquence d'abonnement (monthly, annual, weekly)")
    print("• Source d'acquisition (tm_source)")
    print("• Médium d'acquisition (tm_medium)")
    print("• Moyen de paiement (psp, null=CB)")
    print("• Tranches de revenus (consolidated_revenues_ht_euro)")
    print("\nPour lancer l'analyse complète, exécutez:")
    print("python retention_analysis.py")
    print("\nOu depuis Python:")
    print("monthly_df, retention_df, summary_df, segmented_results, segment_summaries = main()")
    
    # Décommenter la ligne suivante pour lancer automatiquement
    main()