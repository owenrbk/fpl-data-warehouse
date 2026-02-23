# FPL Data Warehouse Project

## Overview

This project is a Fantasy Premier League (FPL) data warehouse built to collect, organize, and analyze FPL and FotMob data. 

It starts by pulling raw JSON data from the FPL API — including player stats, team info, fixtures, and gameweek performance. Then, all Premier League matches for the current season are pulled from FotMob's APIs. This helps put FPL data into perspective with more detailed stats of each game.

This data is sent into a locally hosted server running a PostgreSQL database inside Docker.

From there, the data is structured into relational tables that make it easy to query, analyze trends, and create dashboards in Power BI.

This server automates the python scripts with cron scheduling, and refreshes final analytics tables with shell scripting & cron scheduling.

## Purpose

Project is a showcase of SQL database design, relational data modeling, data extraction with Python, automation with cron jobs and shell scripts, and analysis and visualization with Power BI.
