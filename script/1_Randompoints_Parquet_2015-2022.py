import pandas as pd
import numpy as np
import os

# Define the sequence of seasons
seasons = ['Summer', 'Autumn', 'Winter', 'Spring']

# Define common x and y points
common_x = [1000]  # Use a single x point for simplicity
common_y = [5000]  # Use a single y point for simplicity

# Generate a dataframe for a single year with one row per season
def create_data_for_year(year, x_values, y_values):
    data = []
    for x in x_values:
        for y in y_values:
            for season in seasons:
                data.append({
                    "x": x,
                    "y": y,
                    "season": season,
                    "year": year,
                    "Aspect": np.random.uniform(-1, 1),
                    "clay": np.random.uniform(20, 50),
                    "NVIS": np.random.choice([0, 1]),
                    "Sand": np.random.uniform(10, 70),
                    "Slope": np.random.uniform(0, 30),
                    "TWI": np.random.uniform(5, 10),
                    "PET": np.random.uniform(50, 100),
                    "Rain": np.random.uniform(50, 200),
                    "Tavg": np.random.uniform(10, 30),
                    "z_score": np.random.uniform(-3, 3)
                })
    return pd.DataFrame(data)

# Directory to save the Parquet files
output_dir = "generated_parquet_files_four_rows"
os.makedirs(output_dir, exist_ok=True)

# Generate Parquet files for each year from 2015 to 2022 with 4 rows (one per season)
for year in range(2015, 2023):
    df = create_data_for_year(year, common_x, common_y)
    file_path = os.path.join(output_dir, f"{year}_weather_data.parquet")
    df.to_parquet(file_path, index=False)
    print(f"Generated Parquet file for year {year} with 4 rows at: {file_path}")