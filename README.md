# FPL Data Warehouse Project

## Overview

This project is a Fantasy Premier League (FPL) data warehouse built to collect, organize, and analyze FPL data. 
It starts by pulling raw JSON data from the FPL API — including player stats, team info, fixtures, and gameweek performance — and loading it into a PostgreSQL database. 
From there, the data is structured into relational tables that make it easy to query, analyze trends, and create dashboards in Power BI.

## Background

Raw JSON data is first stored as-is, then parsed into tables such as players, teams, fixtures, and player history. 
This workflow enables tracking player performance over time, analyzing manager trends, and building analytics-ready views without touching the raw data each time.

## Plans

Once the cloud version is fully developed and the workflow is finalized, the plan is to migrate the database to an old computer in my local environment. 
This allows for long-term control of the data and experimentation with the full ETL process locally, along with the creation of local and cloud backups for redundancy. 
The project demonstrates the complete cycle of handling real-world API data: ingestion, cleaning, structuring, and preparing it for analysis, both in the cloud and on a personal machine.

## Purpose

Project is a showcase of SQL database design, relational data modeling, ETL workflows, cloud database usage, local database migration, and preparing datasets for analysis and visualization.
