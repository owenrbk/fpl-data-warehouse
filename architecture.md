# Project Architecture

The FPL Data Warehouse is designed to efficiently handle, store, and prepare Fantasy Premier League (FPL) data for analysis and visualization. The architecture follows a multi-layer, medallion architecture approach inspired by modern data engineering best practices.

1. **Data Ingestion (Bronze Layer)**  
   Raw JSON data is pulled from the FPL API, including information about players, teams, fixtures, and gameweek performance. This raw data is stored exactly as received, ensuring a permanent, immutable copy of the source.

3. **Data Transformation (Silver Layer)**  
   Using SQL & Python ETL scripts, the raw JSON is parsed and structured into relational tables such as players, teams, fixtures, and player history.
   This layer organizes the data for efficient querying and ensures that calculations and joins can be performed without repeatedly accessing the raw JSON.  

5. **Analytics and Dashboard Layer (Gold Layer)**  
   From the structured tables, views and summary tables are created to support analytics and visualization.
   Examples include player performance over time, team trend metrics, and weekly fixture analysis.
   This layer is ready for tools like Tableau or Excel and enables insights without touching the raw data again.

7. **Local Migration**  
   Once the project workflow is finalized, the database will be migrated to a local machine for full control and offline experimentation.
   The cloud environment remains as a backup and testing ground for ETL development.
