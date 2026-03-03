"""
transform.py
Cleans & transforms raw healthcare data
"""

import pandas as pd
import json
from datetime import datetime

def transform_data(input_file):
    """Transform raw JSON data to clean CSV"""

    # Load JSON
    with open(input_file, 'r') as f:
        data = json.load(f)

    # convert to dataframe
    # this part depends on API structure
    df = pd.DataFrame(data)

    # clean column names (lowercase, remove spaces)
    df.columns = df.columns.str.lower().str.replace(' ','_')

    # handle missing values
    df = df.fillna(0) # or df.dropna(), dependin on your needs

    # add any calculated fields
    # df['new_column'] = df['old_column']*2

    # save to csv
    output_file = 'transformed_data.csv'
    df.to_csv(output_file, index=False)

    print(f"Data transformed: {output_file}")
    print(f"Shape: {df.shape}")
    return df

if __name__ == "__main__":
    transform_data('raw_data_[TIMESTAMP].json')
