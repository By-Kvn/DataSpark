"""
Streamlit Dashboard pour visualiser les données de la couche Gold.
Affiche les KPIs, agrégations et statistiques du pipeline ETL.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from pathlib import Path

from flows.config import BUCKET_GOLD, get_minio_client

# Configuration de la page
st.set_page_config(
    page_title="DataSpark Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre principal
st.title("📊 DataSpark - Dashboard Analytique")
st.markdown("Visualisation des données de la couche **Gold** - KPIs et Agrégations")

# Fonction pour télécharger depuis MinIO
@st.cache_data(ttl=3600)  # Cache pour 1 heure
def load_data_from_gold(object_name: str) -> pd.DataFrame:
    """
    Télécharge un fichier Parquet depuis le bucket Gold de MinIO.
    
    Args:
        object_name: Nom du fichier dans le bucket gold
        
    Returns:
        DataFrame avec les données
    """
    try:
        client = get_minio_client()
        
        if not client.bucket_exists(BUCKET_GOLD):
            st.error(f"Bucket {BUCKET_GOLD} n'existe pas dans MinIO")
            return pd.DataFrame()
        
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        df = pd.read_parquet(BytesIO(data))
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement de {object_name}: {str(e)}")
        return pd.DataFrame()


# Sidebar pour la navigation
st.sidebar.title("📑 Navigation")
page = st.sidebar.selectbox(
    "Choisir une section",
    ["🏠 Vue d'ensemble", "📈 KPIs", "🌍 Géographie", "📅 Temporel", "📊 Dimensions"]
)

# Page 1: Vue d'ensemble
if page == "🏠 Vue d'ensemble":
    st.header("Vue d'ensemble du Pipeline")
    
    # Métriques principales
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Charger les données de base
        fact_achats = load_data_from_gold("fact_achats.parquet")
        dim_client = load_data_from_gold("dim_client.parquet")
        kpi_distributions = load_data_from_gold("kpi_distributions_statistiques.parquet")
        
        if not fact_achats.empty:
            total_transactions = len(fact_achats)
            total_ca = fact_achats['montant'].sum()
            total_clients = len(dim_client) if not dim_client.empty else 0
            panier_moyen = fact_achats['montant'].mean() if not fact_achats.empty else 0
            
            with col1:
                st.metric("💰 Chiffre d'Affaires Total", f"{total_ca:,.0f} €")
            with col2:
                st.metric("🛒 Transactions", f"{total_transactions:,}")
            with col3:
                st.metric("👥 Clients Uniques", f"{total_clients:,}")
            with col4:
                st.metric("📦 Panier Moyen", f"{panier_moyen:.2f} €")
            
            # Graphique de répartition du CA
            st.subheader("📊 Répartition du Chiffre d'Affaires")
            
            col_left, col_right = st.columns(2)
            
            with col_left:
                # CA par jour (derniers 30 jours)
                kpi_volumes_jour = load_data_from_gold("kpi_volumes_jour.parquet")
                if not kpi_volumes_jour.empty:
                    kpi_volumes_jour['date'] = pd.to_datetime(kpi_volumes_jour['date'])
                    kpi_volumes_jour = kpi_volumes_jour.sort_values('date')
                    dernier_30j = kpi_volumes_jour.tail(30)
                    
                    fig_ca_jour = px.line(
                        dernier_30j,
                        x='date',
                        y='ca_total',
                        title="CA par Jour (30 derniers jours)",
                        labels={'ca_total': 'CA Total (€)', 'date': 'Date'}
                    )
                    fig_ca_jour.update_traces(line_color='#1f77b4', line_width=2)
                    st.plotly_chart(fig_ca_jour, use_container_width=True)
            
            with col_right:
                # CA par mois
                kpi_volumes_mois = load_data_from_gold("kpi_volumes_mois.parquet")
                if not kpi_volumes_mois.empty:
                    kpi_volumes_mois['periode'] = kpi_volumes_mois.apply(
                        lambda x: f"{int(x['annee'])}-{int(x['mois']):02d}", axis=1
                    )
                    
                    fig_ca_mois = px.bar(
                        kpi_volumes_mois,
                        x='periode',
                        y='ca_total',
                        title="CA par Mois",
                        labels={'ca_total': 'CA Total (€)', 'periode': 'Période'}
                    )
                    fig_ca_mois.update_traces(marker_color='#2ca02c')
                    st.plotly_chart(fig_ca_mois, use_container_width=True)
            
            # Statistiques de distribution
            if not kpi_distributions.empty:
                st.subheader("📈 Statistiques de Distribution")
                st.dataframe(kpi_distributions, use_container_width=True)
        else:
            st.warning("⚠️ Aucune donnée disponible. Exécutez d'abord le pipeline ETL.")
            
    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {str(e)}")
        st.info("💡 Assurez-vous que le pipeline ETL a été exécuté et que MinIO est accessible.")


# Page 2: KPIs
elif page == "📈 KPIs":
    st.header("Indicateurs de Performance (KPIs)")
    
    try:
        # KPI: Volumes par période
        st.subheader("📅 Volumes de Transactions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            kpi_volumes_jour = load_data_from_gold("kpi_volumes_jour.parquet")
            if not kpi_volumes_jour.empty:
                kpi_volumes_jour['date'] = pd.to_datetime(kpi_volumes_jour['date'])
                fig = px.scatter(
                    kpi_volumes_jour,
                    x='date',
                    y='nb_achats',
                    size='ca_total',
                    color='ca_total',
                    title="Volume de Transactions par Jour",
                    labels={'nb_achats': 'Nombre d\'achats', 'date': 'Date', 'ca_total': 'CA (€)'},
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            kpi_volumes_semaine = load_data_from_gold("kpi_volumes_semaine.parquet")
            if not kpi_volumes_semaine.empty:
                kpi_volumes_semaine['periode'] = kpi_volumes_semaine.apply(
                    lambda x: f"S{int(x['semaine'])}-{int(x['annee'])}", axis=1
                )
                fig = px.bar(
                    kpi_volumes_semaine,
                    x='periode',
                    y='nb_achats',
                    title="Volume de Transactions par Semaine",
                    labels={'nb_achats': 'Nombre d\'achats', 'periode': 'Semaine'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # KPI: Taux de croissance
        st.subheader("📈 Taux de Croissance Mensuel")
        kpi_taux_croissance = load_data_from_gold("kpi_taux_croissance.parquet")
        if not kpi_taux_croissance.empty:
            kpi_taux_croissance['periode'] = kpi_taux_croissance.apply(
                lambda x: f"{int(x['annee'])}-{int(x['mois']):02d}", axis=1
            )
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=kpi_taux_croissance['periode'],
                y=kpi_taux_croissance['taux_croissance'],
                name='Taux de croissance (%)',
                marker_color=kpi_taux_croissance['taux_croissance'].apply(
                    lambda x: 'green' if x > 0 else 'red'
                )
            ))
            fig.update_layout(
                title="Taux de Croissance Mensuel (%)",
                xaxis_title="Période",
                yaxis_title="Taux de croissance (%)"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(kpi_taux_croissance[['periode', 'ca', 'taux_croissance']], use_container_width=True)
        
    except Exception as e:
        st.error(f"Erreur: {str(e)}")


# Page 3: Géographie
elif page == "🌍 Géographie":
    st.header("Analyse Géographique")
    
    try:
        # CA par pays
        st.subheader("💰 Chiffre d'Affaires par Pays")
        kpi_ca_pays = load_data_from_gold("kpi_ca_par_pays.parquet")
        
        if not kpi_ca_pays.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Graphique en barres
                fig_bar = px.bar(
                    kpi_ca_pays,
                    x='pays',
                    y='ca_total',
                    title="CA Total par Pays",
                    labels={'ca_total': 'CA Total (€)', 'pays': 'Pays'},
                    color='ca_total',
                    color_continuous_scale='Blues'
                )
                fig_bar.update_xaxes(tickangle=45)
                st.plotly_chart(fig_bar, use_container_width=True)
            
            with col2:
                # Graphique en camembert
                fig_pie = px.pie(
                    kpi_ca_pays,
                    values='ca_total',
                    names='pays',
                    title="Répartition du CA par Pays (%)"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            # Tableau détaillé
            st.subheader("📋 Détails par Pays")
            st.dataframe(
                kpi_ca_pays[['pays', 'ca_total', 'ca_moyen', 'nb_achats']].style.format({
                    'ca_total': '{:,.0f} €',
                    'ca_moyen': '{:.2f} €',
                    'nb_achats': '{:,}'
                }),
                use_container_width=True
            )
        else:
            st.warning("Aucune donnée géographique disponible.")
            
    except Exception as e:
        st.error(f"Erreur: {str(e)}")


# Page 4: Temporel
elif page == "📅 Temporel":
    st.header("Analyse Temporelle")
    
    try:
        # Agrégations temporelles
        st.subheader("📊 Agrégations Temporelles")
        
        agg_type = st.selectbox(
            "Choisir la granularité",
            ["Jour", "Semaine", "Mois"]
        )
        
        if agg_type == "Jour":
            agg_data = load_data_from_gold("agregation_jour.parquet")
            if not agg_data.empty:
                agg_data['date'] = pd.to_datetime(agg_data['date'])
                agg_data = agg_data.sort_values('date')
                
                fig = px.line(
                    agg_data,
                    x='date',
                    y='ca_total',
                    title="CA Total par Jour",
                    labels={'ca_total': 'CA Total (€)', 'date': 'Date'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(agg_data, use_container_width=True)
        
        elif agg_type == "Semaine":
            agg_data = load_data_from_gold("agregation_semaine.parquet")
            if not agg_data.empty:
                agg_data['periode'] = agg_data.apply(
                    lambda x: f"S{int(x['semaine'])}-{int(x['annee'])}", axis=1
                )
                
                fig = px.bar(
                    agg_data,
                    x='periode',
                    y='ca_total',
                    title="CA Total par Semaine",
                    labels={'ca_total': 'CA Total (€)', 'periode': 'Semaine'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(agg_data, use_container_width=True)
        
        elif agg_type == "Mois":
            agg_data = load_data_from_gold("agregation_mois.parquet")
            if not agg_data.empty:
                agg_data['periode'] = agg_data.apply(
                    lambda x: f"{int(x['annee'])}-{int(x['mois']):02d}", axis=1
                )
                
                fig = px.bar(
                    agg_data,
                    x='periode',
                    y='ca_total',
                    title="CA Total par Mois",
                    labels={'ca_total': 'CA Total (€)', 'periode': 'Mois'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(agg_data, use_container_width=True)
                
    except Exception as e:
        st.error(f"Erreur: {str(e)}")


# Page 5: Dimensions
elif page == "📊 Dimensions":
    st.header("Tables de Dimension")
    
    try:
        dim_type = st.selectbox(
            "Choisir une dimension",
            ["Clients", "Produits", "Temps", "Pays"]
        )
        
        if dim_type == "Clients":
            dim_data = load_data_from_gold("dim_client.parquet")
            st.subheader("👥 Dimension Clients")
            if not dim_data.empty:
                st.metric("Nombre de clients", len(dim_data))
                st.dataframe(dim_data.head(100), use_container_width=True)
                
                # Répartition par pays
                if 'pays' in dim_data.columns:
                    pays_count = dim_data['pays'].value_counts()
                    fig = px.pie(
                        values=pays_count.values,
                        names=pays_count.index,
                        title="Répartition des Clients par Pays"
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        elif dim_type == "Produits":
            dim_data = load_data_from_gold("dim_produit.parquet")
            st.subheader("🛍️ Dimension Produits")
            if not dim_data.empty:
                st.metric("Nombre de produits", len(dim_data))
                st.dataframe(dim_data, use_container_width=True)
        
        elif dim_type == "Temps":
            dim_data = load_data_from_gold("dim_temps.parquet")
            st.subheader("📅 Dimension Temps")
            if not dim_data.empty:
                st.metric("Nombre de jours uniques", len(dim_data))
                st.dataframe(dim_data.head(100), use_container_width=True)
        
        elif dim_type == "Pays":
            dim_data = load_data_from_gold("dim_pays.parquet")
            st.subheader("🌍 Dimension Pays")
            if not dim_data.empty:
                st.metric("Nombre de pays", len(dim_data))
                st.dataframe(dim_data, use_container_width=True)
                
    except Exception as e:
        st.error(f"Erreur: {str(e)}")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ Information")
st.sidebar.info(
    "Ce dashboard affiche les données de la couche **Gold** "
    "du pipeline ETL DataSpark.\n\n"
    "Les données sont chargées depuis MinIO."
)

# Instructions pour lancer
st.sidebar.markdown("### 🚀 Lancer le Dashboard")
st.sidebar.code("streamlit run streamlit_app.py", language="bash")
