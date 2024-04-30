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

def accepting_new_data(new_data, current_data):
    #readings is a list of dictionary, each one is a reading of the air quality 

    latest_current_df = current_data.groupby(['name', 'location'])['end'].max()

    kept_data = new_data.copy()

    for index in new_data.index:
        if new_data.loc[index,'end'] <= latest_current_df.loc[new_data.loc[index,'name']
                                                                          [new_data.loc[index,'location']],'end'] :
            kept_data.drop(index, axis='index')
    
    return kept_data