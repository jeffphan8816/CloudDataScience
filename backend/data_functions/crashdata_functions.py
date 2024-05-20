import pandas as pd
import os

def get_crash_data(file_path: str) -> list:
    """
    Load a cleaned dataframe of crash data

    @param file_path: str
    @return pd.DataFrame
    """

    # Check that file exists
    if not os.path.isfile(file_path):
        raise ValueError

    # Read data and clean
    df = pd.read_csv(file_path)
    df = df.rename(columns={' crash_date': 'crash_date', ' latitude': 'latitude',
                            'light_condition': 'light_condition', ' longitude': 'longitude', ' severity': 'severity'})

    to_keep = ['crash_date', 'latitude',
               'light_condition', 'longitude', 'severity']
    df_reduced = df.drop(set(df.columns)-set(to_keep), axis=1)

    # Try to convert severity to int
    severity_dict = {'Not known': -1, 'Property Damage Only': 0,
                     'Minor': 1, 'First Aid': 2, 'Serious': 3, 'Fatal': 4}
    try:
        df_reduced['severity'] = df_reduced['severity'].apply(
            lambda severity: severity_dict[severity])
    except KeyError:
        pass  # TODO  add in logs

    return df_reduced.to_dict(orient='records')
