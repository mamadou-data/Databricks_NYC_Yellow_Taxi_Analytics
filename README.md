# 🚕 NYC Yellow Taxi Analytics — Databricks Medallion & Power BI

## 📌 Présentation du projet

Ce projet a pour objectif d’analyser les données NYC Yellow Taxi (2025) à l’aide d’une architecture Medallion (Bronze / Silver / Gold) implémentée sur Databricks, avec une orchestration automatisée via Databricks Workflows, et une visualisation avancée dans Power BI.

L’objectif est de démontrer une approche end-to-end Data Analytics / Analytics Engineering, depuis l’ingestion des données brutes jusqu’à la prise de décision métier via des dashboards interactifs.

---

## 🎯 Objectifs métiers

Analyser les volumes de courses et les revenus des taxis new-yorkais
- Identifier les zones et horaires les plus actifs
- Comparer les flux Pickup vs Dropoff
- Étudier les comportements de paiement et les pourboires
- Mettre en place un monitoring de la qualité des données
- Fournir des indicateurs fiables pour l’aide à la décision

---

## 🏗️ Architecture globale

🔹 Architecture Medallion

- Bronze : données brutes issues des fichiers Parquet TLC

- Silver : données nettoyées, normalisées et enrichies

- Gold : modèle analytique optimisé pour la BI (schéma étoile)

🔹 Orchestration

* Databricks Workflows pour exécuter automatiquement l’ensemble du pipeline :

   1. Ingestion Bronze

   2. Nettoyage Silver

   3. Modélisation Gold

🔹 Visualisation

- Power BI connecté à Databricks SQL Warehouse

- Modèle sémantique optimisé via des vues vw_

## 🧱 Stack technique

- Databricks (Spark, Delta Lake, Unity Catalog)

- Python / PySpark

- SQL

- Databricks Workflows

- Power BI (Import mode)

- GitHub (versioning & documentation)

---

## 📂 Structure du projet
```
NYC_Yellow_Taxi_Analytics/
│
├── notebooks/
│   ├── 02_bronze_ingestion_2025.py
│   ├── 03_silver_clean.py
│   ├── 04_gold_final.py
│
├── nyc_taxi_data/
│   └── bronze_data/
│       ├── yellow_tripdata_2025-01.parquet
│       └── taxi_zone_lookup.csv
│
├── power_bi/
│   └── NYC_Yellow_Taxi_Analytics.pbix
│
├── README.md
```
---
## 🟫 Bronze Layer — Ingestion

🎯 Objectif

- Charger les fichiers Parquet bruts (2025)
- Garantir une ingestion idempotente
- Tracer les chargements via un ingestion log

🔹 Tables

- bronze_db.raw_trips
- bronze_db.ingestion_log

🔹 Points clés

- Lecture directe depuis DBFS Workspace
- Conservation du type timestamp_ntz
- Ajout de métadonnées techniques :
```
   ingestion_timestamp
   file_name
   file_path
```
  
---

## ⚪ Silver Layer — Nettoyage & Qualité

🎯 Objectif

- Nettoyer et standardiser les données
- Créer des indicateurs de qualité
- Préparer les données pour l’analyse

🔹 Table
```
trips_clean
```
🔹 Transformations principales

- Normalisation des noms de colonnes
- Calcul de la durée de trajet
- Extraction de la date et de l’heure
- Règles de qualité :
   - durée > 0
   - distance ≥ 0
   - montant total > 0

- Flag final : ```is_valid_trip```

🟨 Gold Layer — Modèle analytique
🎯 Objectif

Fournir un modèle optimisé pour Power BI

Implémenter un schéma étoile

🔹 Tables
```
gold_db.fact_trips

gold_db.dim_date

gold_db.dim_zone

gold_db.dim_payment_type

gold_db.kpi_daily
```

🔹 Vues BI (contract layer)
```
vw_fact_trips

vw_dim_date

vw_dim_zone

vw_dim_payment_type

vw_kpi_daily
```
👉 Ces vues servent de point d’entrée unique pour Power BI.

---

## 🔁 Orchestration — Databricks Workflow

🔹 Pipeline automatisé

1. Bronze ingestion

2. Silver clean

3. Gold final

🔹 Bonnes pratiques

- Dépendances entre tâches

- Relance contrôlée

- Pipeline idempotent

- Exécution manuelle ou planifiée

## 📊 Power BI — Dashboard Analytics

🔹 Modèle de données

- Schéma étoile

- Relation active : Pickup

- Relation inactive : Dropoff (gérée via DAX USERELATIONSHIP)

🔹 Pages du rapport

1. Overview

- KPIs : Trips, Revenue, Avg Revenue / Trip, Distance, Durée, Tip Rate

- Tendance temporelle avec moyenne mobile 7 jours

2. Zones & Flows

- Analyse Pickup vs Dropoff

- Top zones

Analyse par Borough

3. Time & Patterns

- Activité par heure

4. Data Quality

- Distance nulle

- Montant ≤ 0

- Durée ≤ 0

📈 **Exemples d’insights**

- Plus de 43 millions de courses analysées en 2025

- 1,19 milliard $ de chiffre d’affaires

- 65 % des paiements par carte bancaire

- Activité maximale entre 15h et 20h

- Environ 2,7 % des courses avec distance nulle (qualité des données)

🚀 **Points forts du projet**

- Architecture scalable et industrielle

- Séparation claire ingestion / transformation / analytics

- Orchestration automatisée

- Dashboard Power BI prêt pour un contexte entreprise

- Projet entièrement reproductible

📌 **Améliorations possibles**

- Ingestion automatique via stockage cloud (ADLS / S3)

- Ajout de données multi-années

- Optimisation des performances (partitionnement Delta)

- Publication Power BI Service + RLS

## 👤 Auteur

**Mamadou DIEDHIOU**

Data Analyst / BI / Analytics Engineer

🔗 [LinkedIn](https://www.linkedin.com/in/diedhiou/)

## 🏁 Conclusion

Ce projet illustre une approche complète Data Analytics moderne, combinant Data Engineering, Analytics et Visualisation, avec des standards proches du monde professionnel.
