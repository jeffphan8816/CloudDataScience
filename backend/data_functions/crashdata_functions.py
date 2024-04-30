import pandas as pd

def clean_dataframe(file_path):
    df = pd.read_csv(file_path)

    to_keep = ['crash_date', 'latitude', 'light_condition', 'longitude', 'severity']
    df.drop(set(df.columns)-set(to_keep), axis=1)

    severity_dict = {'Not known':-1, 'Property Damage Only':0, 'Minor':1, 'First Aid':2, 'Serious':3, 'Fatal':4}
    try :
        df['severity'].apply(lambda severity : severity_dict[severity])
    except KeyError :
        pass           #TODO  add in logs
