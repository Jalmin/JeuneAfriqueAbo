#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard Local - Analyse de Rétention des Abonnés
Ce script crée un tableau de bord web interactif en local avec Plotly Dash
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, callback_context
import dash_bootstrap_components as dbc
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
DATA_FILE = 'analyse_retention_segmentee.xlsx'
PORT = 8050

def load_retention_data():
    """Charge les données d'analyse de rétention depuis le fichier Excel"""
    print("Chargement des données de rétention...")
    
    try:
        # Charger les différents onglets
        retention_global = pd.read_excel(DATA_FILE, sheet_name='Retention_Globale')
        resume_cohortes = pd.read_excel(DATA_FILE, sheet_name='Resume_Cohortes')
        
        # Charger les données segmentées
        segments_data = {}
        segment_names = ['Retention_Frequence', 'Retention_Tm_source', 'Retention_Tm_medium', 
                        'Retention_Psp', 'Retention_Revenue_tranche']
        
        for segment_name in segment_names:
            try:
                segment_df = pd.read_excel(DATA_FILE, sheet_name=segment_name)
                segments_data[segment_name] = segment_df
                print(f"✓ {segment_name}: {len(segment_df)} lignes, colonnes: {list(segment_df.columns)}")
                
                # Afficher les valeurs uniques de segment_value
                if 'segment_value' in segment_df.columns:
                    unique_values = segment_df['segment_value'].unique()
                    print(f"  Valeurs uniques: {unique_values}")
                else:
                    print(f"  ⚠️ Colonne 'segment_value' manquante!")
                    
            except Exception as e:
                print(f"⚠️ Onglet {segment_name} non trouvé: {e}")
        
        print(f"✓ Données chargées: {len(retention_global)} lignes de rétention globale")
        print(f"✓ {len(resume_cohortes)} cohortes analysées")
        print(f"✓ {len(segments_data)} segments chargés")
        
        return retention_global, resume_cohortes, segments_data
    
    except Exception as e:
        print(f"ERREUR: Impossible de charger {DATA_FILE}")
        print(f"Détail: {e}")
        print("\nAssurez-vous d'avoir d'abord exécuté le script d'analyse de rétention.")
        return None, None, None

def prepare_retention_chart_data(retention_df, selected_cohort='all'):
    """Prépare les données pour le graphique de rétention"""
    if retention_df is None or retention_df.empty:
        return []
    
    months = list(range(26))  # 0 à 25 mois
    chart_data = []
    
    for month in months:
        month_data = {'mois_relatif': month}
        
        if selected_cohort == 'all':
            # Moyenne de toutes les cohortes
            cohort_data = retention_df[retention_df['mois_relatif'] == month]
            if not cohort_data.empty:
                avg_retention = cohort_data['taux_retention'].mean()
                month_data['retention_moyenne'] = round(avg_retention, 2)
        else:
            # Cohorte spécifique
            cohort_data = retention_df[
                (retention_df['cohorte'] == selected_cohort) & 
                (retention_df['mois_relatif'] == month)
            ]
            if not cohort_data.empty:
                month_data[selected_cohort] = cohort_data['taux_retention'].iloc[0]
        
        chart_data.append(month_data)
    
    return chart_data

def prepare_segment_evolution_data(segments_data, segment_type, cohort_filter='all'):
    """Prépare les données pour l'évolution de la rétention par segment avec filtre de cohorte"""
    print(f"\n=== DIAGNOSTIC {segment_type} (Filtre: {cohort_filter}) ===")
    
    if segment_type not in segments_data:
        print(f"❌ Segment {segment_type} non trouvé")
        return [], []
    
    segment_df = segments_data[segment_type].copy()
    print(f"✓ Données initiales: {len(segment_df)} lignes")
    
    # ÉTAPE 1: FILTRER LES COHORTES D'ABORD
    if cohort_filter != 'all':
        try:
            if 'cohorte' in segment_df.columns:
                segment_df['cohorte_date'] = pd.to_datetime(segment_df['cohorte'], format='%m/%Y')
                
                # Définir la date limite selon le filtre
                if cohort_filter == '2023-12':
                    cutoff_date = pd.to_datetime('2023-12-01')
                    max_months = 25  # On peut voir jusqu'à M25 avec ces cohortes
                elif cohort_filter == '2023-06':
                    cutoff_date = pd.to_datetime('2023-06-01')
                    max_months = 25
                elif cohort_filter == '2022-12':
                    cutoff_date = pd.to_datetime('2022-12-01')
                    max_months = 25
                elif cohort_filter == '2022-06':
                    cutoff_date = pd.to_datetime('2022-06-01')
                    max_months = 25
                else:
                    cutoff_date = pd.to_datetime('2099-12-01')
                    max_months = 25
                
                # FILTRER UNIQUEMENT LES COHORTES (pas les mois)
                cohortes_avant = segment_df['cohorte'].nunique()
                segment_df = segment_df[segment_df['cohorte_date'] <= cutoff_date]
                cohortes_apres = segment_df['cohorte'].nunique()
                
                print(f"✓ Cohortes filtrées: {cohortes_avant} → {cohortes_apres} cohortes")
                print(f"✓ Données après filtrage cohorte: {len(segment_df)} lignes")
                
                # Afficher les cohortes retenues
                cohortes_retenues = sorted(segment_df['cohorte'].unique())
                print(f"✓ Cohortes retenues: {cohortes_retenues[:5]}...{cohortes_retenues[-3:] if len(cohortes_retenues) > 8 else ''}")
            else:
                print("⚠️ Colonne 'cohorte' non trouvée, pas de filtrage possible")
                max_months = 25
        except Exception as e:
            print(f"⚠️ Erreur lors du filtrage: {e}")
            max_months = 25
    else:
        max_months = 25
    
    # ÉTAPE 2: NETTOYER LES DONNÉES
    required_cols = ['segment_value', 'mois_relatif', 'clients_initiaux', 'clients_actifs']
    for col in required_cols:
        if col not in segment_df.columns:
            print(f"❌ Colonne '{col}' manquante!")
            return [], []
    
    segment_df = segment_df[segment_df['clients_initiaux'] > 0]
    segment_df = segment_df[segment_df['clients_actifs'] >= 0]
    segment_df = segment_df[segment_df['mois_relatif'] <= max_months]
    
    print(f"✓ Données nettoyées: {len(segment_df)} lignes valides")
    
    # ÉTAPE 3: RECALCULER LES TOTAUX PAR SEGMENT AVEC LES COHORTES FILTRÉES
    print(f"\n--- RECALCUL DES TOTAUX AVEC COHORTES FILTRÉES ---")
    
    # Identifier les segments principaux par taille (APRÈS filtrage)
    segment_sizes = segment_df[segment_df['mois_relatif'] == 0].groupby('segment_value')['clients_initiaux'].sum()
    main_segments = segment_sizes.nlargest(3).index.tolist()
    
    print(f"✓ Top 3 segments (après filtrage): {main_segments}")
    for seg in main_segments:
        size = segment_sizes[seg]
        print(f"  - {seg}: {size:,} clients")
    
    if not main_segments:
        return [], []
    
    # ÉTAPE 4: CALCULER L'ÉVOLUTION POUR CHAQUE MOIS
    evolution_data = []
    months = list(range(0, max_months + 1))
    
    for month in months:
        month_data = {'mois_relatif': month}
        
        for segment_value in main_segments:
            # 1. TOTAL clients initiaux pour ce segment (M0, toutes cohortes filtrées)
            initial_data = segment_df[
                (segment_df['segment_value'] == segment_value) & 
                (segment_df['mois_relatif'] == 0)
            ]
            
            if initial_data.empty:
                month_data[str(segment_value)] = None
                continue
                
            total_initial = initial_data['clients_initiaux'].sum()
            
            # 2. TOTAL clients actifs pour ce segment à ce mois (toutes cohortes filtrées)
            month_data_seg = segment_df[
                (segment_df['segment_value'] == segment_value) & 
                (segment_df['mois_relatif'] == month)
            ]
            
            if month_data_seg.empty:
                month_data[str(segment_value)] = None
                continue
            
            total_active = month_data_seg['clients_actifs'].sum()
            
            # 3. Calculer la rétention sur le nouveau bassin
            if total_initial > 0:
                retention_rate = (total_active / total_initial) * 100
                retention_rate = min(retention_rate, 100.0)
                
                if month == 0:
                    retention_rate = 100.0
                
                month_data[str(segment_value)] = round(retention_rate, 1)
                
                # Debug pour certains mois
                if month in [0, 6, 12, 18, 24] and month <= max_months:
                    print(f"  {segment_value} M{month}: {total_active:,}/{total_initial:,} = {retention_rate:.1f}%")
            else:
                month_data[str(segment_value)] = None
        
        evolution_data.append(month_data)
    
    # ÉTAPE 5: VALIDATION FINALE (lissage optionnel)
    for segment in main_segments:
        values = []
        
        for item in evolution_data:
            val = item.get(str(segment))
            if val is not None:
                values.append(val)
        
        if len(values) > 1:
            print(f"✓ {segment}: {len(values)} points, range {min(values):.1f}%-{max(values):.1f}%")
    
    return evolution_data, main_segments

def create_segment_evolution_chart(evolution_data, main_segments, segment_type):
    """Crée le graphique d'évolution de la rétention par segment"""
    if not evolution_data or not main_segments:
        fig = go.Figure()
        fig.add_annotation(
            text="Pas assez de données valides",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False, font=dict(size=16, color="gray")
        )
        fig.update_layout(title="Données insuffisantes", template='plotly_white', height=400)
        return fig
    
    fig = go.Figure()
    colors = ['#3B82F6', '#10B981', '#F59E0B']  # Bleu, Vert, Orange
    
    for i, segment in enumerate(main_segments):
        x_values = []
        y_values = []
        
        # Collecter les points valides
        for item in evolution_data:
            value = item.get(str(segment))
            if value is not None and 0 <= value <= 100:
                x_values.append(item['mois_relatif'])
                y_values.append(value)
        
        if len(x_values) >= 3:  # Minimum 3 points pour une courbe
            fig.add_trace(go.Scatter(
                x=x_values,
                y=y_values,
                mode='lines+markers',
                name=f'{segment.title()} ({len(x_values)} mois)',
                line=dict(color=colors[i % len(colors)], width=3),
                marker=dict(size=5),
                connectgaps=False
            ))
    
    fig.update_layout(
        title=f'Rétention par {segment_type.replace("Retention_", "").replace("_", " ").title()}',
        xaxis_title='Mois depuis l\'abonnement',
        yaxis_title='Taux de rétention (%)',
        yaxis=dict(range=[0, 105], ticksuffix='%'),
        xaxis=dict(range=[0, 25]),
        template='plotly_white',
        height=400,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=0.95,
            xanchor="left",
            x=1.02
        ),
        margin=dict(r=120)
    )
    
    # Ajouter des lignes de référence
    fig.add_hline(y=50, line_dash="dash", line_color="red", opacity=0.3, 
                  annotation_text="50% rétention")
    fig.add_hline(y=100, line_dash="dot", line_color="green", opacity=0.3,
                  annotation_text="Départ (100%)")
    
    return fig
    """Prépare les données pour l'évolution de la rétention par segment"""
    if segment_type not in segments_data:
        print(f"Segment {segment_type} non trouvé dans les données")
        return [], []
    
    segment_df = segments_data[segment_type]
    print(f"Traitement du segment {segment_type} avec {len(segment_df)} lignes")
    
    # Identifier les segments principaux (avec suffisamment de données)
    segment_sizes = segment_df.groupby('segment_value')['clients_initiaux'].sum()
    print(f"Tailles des segments: {segment_sizes.to_dict()}")
    
    # Prendre tous les segments avec au moins 20 clients
    main_segments = segment_sizes[segment_sizes >= 20].index.tolist()
    print(f"Segments principaux retenus: {main_segments}")
    
    if not main_segments:
        return [], []
    
    evolution_data = []
    months = list(range(0, 26))  # 0 à 25 mois
    
    for month in months:
        month_data = {'mois_relatif': month}
        
        for segment_value in main_segments:
            segment_month_data = segment_df[
                (segment_df['segment_value'] == segment_value) & 
                (segment_df['mois_relatif'] == month)
            ]
            
            if not segment_month_data.empty:
                # Moyenne pondérée par le nombre de clients
                total_clients = segment_month_data['clients_initiaux'].sum()
                weighted_retention = (segment_month_data['taux_retention'] * segment_month_data['clients_initiaux']).sum() / total_clients if total_clients > 0 else 0
                month_data[str(segment_value)] = round(weighted_retention, 2)
            else:
                month_data[str(segment_value)] = None
        
        evolution_data.append(month_data)
    
    print(f"Données d'évolution générées pour {len(evolution_data)} mois")
    return evolution_data, main_segments

def prepare_segment_data(segments_data, segment_type):
    """Prépare les données pour l'analyse par segment (barres)"""
    if segment_type not in segments_data:
        return []
    
    segment_df = segments_data[segment_type]
    
    # Calculer les moyennes par segment pour les mois clés
    summary_data = []
    
    for segment_value in segment_df['segment_value'].unique():
        segment_subset = segment_df[segment_df['segment_value'] == segment_value]
        
        # Taille totale du segment
        total_clients = segment_subset[segment_subset['mois_relatif'] == 0]['clients_initiaux'].sum()
        
        if total_clients < 20:  # Filtrer les segments trop petits
            continue
        
        # Moyennes de rétention
        retention_1m = segment_subset[segment_subset['mois_relatif'] == 1]['taux_retention'].mean()
        retention_12m = segment_subset[segment_subset['mois_relatif'] == 12]['taux_retention'].mean()
        retention_18m = segment_subset[segment_subset['mois_relatif'] == 18]['taux_retention'].mean()
        retention_24m = segment_subset[segment_subset['mois_relatif'] == 24]['taux_retention'].mean()
        
        summary_data.append({
            'segment': str(segment_value),
            'total_clients': total_clients,
            'retention_1m': retention_1m if not pd.isna(retention_1m) else 0,
            'retention_12m': retention_12m if not pd.isna(retention_12m) else 0,
            'retention_18m': retention_18m if not pd.isna(retention_18m) else 0,
            'retention_24m': retention_24m if not pd.isna(retention_24m) else 0
        })
    
    return sorted(summary_data, key=lambda x: x['retention_24m'], reverse=True)

def create_retention_line_chart(chart_data, selected_cohort):
    """Crée le graphique en ligne de rétention"""
    fig = go.Figure()
    
    if selected_cohort == 'all':
        y_values = [item.get('retention_moyenne', 0) for item in chart_data]
        fig.add_trace(go.Scatter(
            x=[item['mois_relatif'] for item in chart_data],
            y=y_values,
            mode='lines+markers',
            name='Rétention Moyenne',
            line=dict(color='#3B82F6', width=3),
            marker=dict(size=6)
        ))
    else:
        y_values = [item.get(selected_cohort, 0) for item in chart_data]
        fig.add_trace(go.Scatter(
            x=[item['mois_relatif'] for item in chart_data],
            y=y_values,
            mode='lines+markers',
            name=f'Cohorte {selected_cohort}',
            line=dict(color='#3B82F6', width=3),
            marker=dict(size=6)
        ))
    
    fig.update_layout(
        title=f'Évolution de la Rétention (M0-M25){" - " + selected_cohort if selected_cohort != "all" else ""}',
        xaxis_title='Mois relatif',
        yaxis_title='Taux de rétention (%)',
        yaxis=dict(range=[0, 100]),
        template='plotly_white',
        height=400
    )
    
    return fig

def create_segment_bar_chart(segment_data, segment_type):
    """Crée le graphique en barres pour les segments"""
    if not segment_data:
        return go.Figure()
    
    df_segment = pd.DataFrame(segment_data)
    
    fig = go.Figure()
    
    # Barres pour chaque période
    fig.add_trace(go.Bar(
        name='Rétention 1M',
        x=df_segment['segment'],
        y=df_segment['retention_1m'],
        marker_color='#3B82F6'
    ))
    
    fig.add_trace(go.Bar(
        name='Rétention 12M',
        x=df_segment['segment'],
        y=df_segment['retention_12m'],
        marker_color='#10B981'
    ))
    
    fig.add_trace(go.Bar(
        name='Rétention 18M',
        x=df_segment['segment'],
        y=df_segment['retention_18m'],
        marker_color='#F59E0B'
    ))
    
    fig.add_trace(go.Bar(
        name='Rétention 24M',
        x=df_segment['segment'],
        y=df_segment['retention_24m'],
        marker_color='#EF4444'
    ))
    
    fig.update_layout(
        title=f'Analyse par {segment_type.replace("Retention_", "").replace("_", " ").title()}',
        xaxis_title='Segment',
        yaxis_title='Taux de rétention (%)',
        yaxis=dict(range=[0, 100]),
        barmode='group',
        template='plotly_white',
        height=400,
        xaxis={'tickangle': 45}
    )
    
    return fig

# Initialiser l'application Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Dashboard Rétention - Jeune Afrique"

# Charger les données
retention_global, resume_cohortes, segments_data = load_retention_data()

if retention_global is None:
    # Page d'erreur si les données ne sont pas disponibles
    app.layout = dbc.Container([
        dbc.Alert([
            html.H4("❌ Données non disponibles", className="alert-heading"),
            html.P(f"Impossible de charger le fichier {DATA_FILE}"),
            html.Hr(),
            html.P("Veuillez d'abord exécuter le script d'analyse de rétention pour générer les données.", className="mb-0")
        ], color="danger")
    ], className="mt-5")
else:
    # Layout principal
    app.layout = dbc.Container([
        # En-tête
        dbc.Row([
            dbc.Col([
                html.H1("📊 Analyse de Rétention des Abonnés", className="text-primary mb-2"),
                html.P("Tableau de bord interactif - Jeune Afrique", className="text-muted"),
                html.Hr()
            ])
        ], className="mb-4"),
        
        # Contrôles
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("🎯 Contrôles", className="card-title"),
                        dbc.Row([
                            dbc.Col([
                                html.Label("Cohorte:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='cohort-dropdown',
                                    options=[{'label': 'Moyenne de toutes les cohortes', 'value': 'all'}] +
                                           [{'label': f"{row['cohorte']} ({int(row['taille_initiale'])} clients)", 
                                             'value': row['cohorte']} 
                                            for _, row in resume_cohortes.iterrows()],
                                    value='all',
                                    className="mb-3"
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Segment:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='segment-dropdown',
                                    options=[
                                        {'label': 'Vue globale', 'value': 'global'},
                                        {'label': '⏰ Fréquence (monthly/annual/weekly)', 'value': 'Retention_Frequence'},
                                        {'label': '📍 Source (tm_source)', 'value': 'Retention_Tm_source'},
                                        {'label': '🔗 Médium (tm_medium)', 'value': 'Retention_Tm_medium'},
                                        {'label': '💳 Moyen de paiement (PSP)', 'value': 'Retention_Psp'},
                                        {'label': '💰 Tranches de revenus', 'value': 'Retention_Revenue_tranche'}
                                    ],
                                    value='Retention_Frequence',
                                    className="mb-3"
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Type de vue:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='view-type-dropdown',
                                    options=[
                                        {'label': '📊 Comparaison (Barres)', 'value': 'comparison'},
                                        {'label': '📈 Évolution (Courbes)', 'value': 'evolution'}
                                    ],
                                    value='comparison',
                                    className="mb-3"
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Cohortes jusqu'à:", className="fw-bold"),
                                dcc.Dropdown(
                                    id='cohort-filter-dropdown',
                                    options=[
                                        {'label': '📅 Toutes les cohortes', 'value': 'all'},
                                        {'label': '🔒 Jusqu\'à 12/2023 (12M+ data)', 'value': '2023-12'},
                                        {'label': '🔒 Jusqu\'à 06/2023 (18M+ data)', 'value': '2023-06'},
                                        {'label': '🔒 Jusqu\'à 12/2022 (24M+ data)', 'value': '2022-12'},
                                        {'label': '🔒 Jusqu\'à 06/2022 (30M+ data)', 'value': '2022-06'}
                                    ],
                                    value='2023-06',  # Par défaut, cohortes avec au moins 18M de data
                                    className="mb-3"
                                )
                            ], width=3)
                        ])
                    ])
                ])
            ])
        ], className="mb-4"),
        
        # Métriques clés
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{len(resume_cohortes)}", className="text-primary"),
                        html.P("Cohortes analysées", className="mb-0")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{resume_cohortes['taille_initiale'].sum():,}", className="text-success"),
                        html.P("Clients totaux", className="mb-0")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{resume_cohortes['retention_1m'].mean():.1f}%", className="text-warning"),
                        html.P("Rétention moy. 1M", className="mb-0")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{resume_cohortes['retention_24m'].mean():.1f}%", className="text-danger"),
                        html.P("Rétention moy. 24M", className="mb-0")
                    ])
                ])
            ], width=3)
        ], className="mb-4"),
        
        # Graphiques
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='retention-line-chart')
                    ])
                ])
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(id='segment-bar-chart')
                    ])
                ])
            ], width=6)
        ], className="mb-4"),
        
        # Tableau de résumé
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("📋 Résumé des Cohortes")),
                    dbc.CardBody([
                        html.Div(id='summary-table')
                    ])
                ])
            ])
        ], className="mb-4"),
        
        # Footer
        dbc.Row([
            dbc.Col([
                html.Hr(),
                html.P(f"Dashboard généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')} | "
                      f"Données: {DATA_FILE}", className="text-muted text-center")
            ])
        ])
        
    ], fluid=True, className="py-3")

# Callbacks pour l'interactivité
@app.callback(
    Output('retention-line-chart', 'figure'),
    Input('cohort-dropdown', 'value')
)
def update_retention_chart(selected_cohort):
    chart_data = prepare_retention_chart_data(retention_global, selected_cohort)
    return create_retention_line_chart(chart_data, selected_cohort)

@app.callback(
    Output('segment-bar-chart', 'figure'),
    [Input('segment-dropdown', 'value'),
     Input('view-type-dropdown', 'value'),
     Input('cohort-filter-dropdown', 'value')]
)
def update_segment_chart(selected_segment, view_type, cohort_filter):
    print(f"Callback déclenché: segment={selected_segment}, view={view_type}, filter={cohort_filter}")
    
    if selected_segment == 'global':
        # Graphique vide avec message
        fig = go.Figure()
        fig.add_annotation(
            text="Sélectionnez un segment pour voir l'analyse détaillée",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False, font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Sélectionnez un segment",
            template='plotly_white',
            height=400
        )
        return fig
    else:
        # Toujours afficher les courbes d'évolution avec le filtre
        evolution_data, main_segments = prepare_segment_evolution_data(segments_data, selected_segment, cohort_filter)
        return create_segment_evolution_chart(evolution_data, main_segments, selected_segment)

@app.callback(
    Output('summary-table', 'children'),
    Input('cohort-dropdown', 'value')
)
def update_summary_table(selected_cohort):
    # Créer un tableau HTML des principales métriques
    if selected_cohort == 'all':
        # Afficher toutes les cohortes
        table_data = resume_cohortes.head(10)
    else:
        # Afficher la cohorte sélectionnée
        table_data = resume_cohortes[resume_cohortes['cohorte'] == selected_cohort]
    
    # Créer le tableau Bootstrap
    table_header = [
        html.Thead([
            html.Tr([
                html.Th("Cohorte"),
                html.Th("Taille"),
                html.Th("1M"),
                html.Th("3M"),
                html.Th("6M"),
                html.Th("12M"),
                html.Th("18M"),
                html.Th("24M"),
            ])
        ])
    ]
    
    table_rows = []
    for _, row in table_data.iterrows():
        table_rows.append(
            html.Tr([
                html.Td(row['cohorte']),
                html.Td(f"{int(row['taille_initiale']):,}"),
                html.Td(f"{row['retention_1m']:.1f}%"),
                html.Td(f"{row['retention_3m']:.1f}%"),
                html.Td(f"{row['retention_6m']:.1f}%"),
                html.Td(f"{row['retention_12m']:.1f}%"),
                html.Td(f"{row['retention_18m']:.1f}%"),
                html.Td(f"{row['retention_24m']:.1f}%"),
            ])
        )
    
    table_body = [html.Tbody(table_rows)]
    
    return dbc.Table(table_header + table_body, striped=True, bordered=True, hover=True, size="sm")

if __name__ == '__main__':
    if retention_global is not None:
        print("\n🚀 DASHBOARD DE RÉTENTION LANCÉ")
        print("=" * 50)
        print(f"📊 Cohortes analysées: {len(resume_cohortes)}")
        print(f"👥 Clients totaux: {resume_cohortes['taille_initiale'].sum():,}")
        print(f"📈 Segments disponibles: {len(segments_data)}")
        print("=" * 50)
        print(f"🌐 Accédez au dashboard sur: http://localhost:{PORT}")
        print("⏹️  Appuyez sur Ctrl+C pour arrêter")
        print("=" * 50)
    
    app.run(debug=False, port=PORT, host='127.0.0.1')