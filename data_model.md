# Data Model

The FPL Data Warehouse uses a relational data model designed to capture all relevant aspects of Fantasy Premier League data. The model is organized to support both historical analysis and ongoing updates from the FPL API.

## Core Tables

- **raw_fpl**  
  Stores the full JSON payload as received from the API. Serves as the immutable source for all transformations.

- **players**  
  Contains detailed information about each player, including name, position, team, cost, and historical statistics. This table is derived by parsing the `elements` section of the raw JSON.

- **teams**  
  Stores team information such as team name, code, and short name. Extracted from the `teams` section of the raw JSON.

- **fixtures**  
  Contains all scheduled matches, including home and away teams, kickoff time, and scores. Derived from the `fixtures` JSON data.

- **player_history**  
  Tracks historical performance for each player by gameweek, including points, goals, assists, and other statistics. Allows for trend analysis and performance tracking over time.

## Relationships

- Each **player** belongs to a single **team**.  
- Each **fixture** involves two teams (home and away).  
- Each **player_history** record links to a player and the corresponding fixture, providing granular performance tracking.

This data model ensures that the raw JSON data is efficiently transformed into structured, queryable tables. It supports both immediate analytics and future expansion for dashboarding or predictive modeling.
