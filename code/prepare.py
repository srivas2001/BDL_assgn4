import pandas as pd
import yaml
import os
def prepare_pred(link,out):
    csv_files=[file for file in os.listdir(link) if file.endswith('.csv')]
    '''monthly_columns = ['MonthlyDepartureFromNormalAverageTemperature',
                   'MonthlyDepartureFromNormalCoolingDegreeDays',
                   'MonthlyDepartureFromNormalHeatingDegreeDays',
                   'MonthlyDepartureFromNormalMaximumTemperature',
                   'MonthlyDepartureFromNormalMinimumTemperature',
                   'MonthlyMaximumTemperature',
                   'MonthlyMeanTemperature',
                   'MonthlyMinimumTemperature'] '''
    for file_name in csv_files:
        input_file_path=os.path.join(link,file_name)
        dataframe=pd.read_csv(input_file_path)
        dataframe['DATE']=pd.to_datetime(dataframe['DATE'])
        dataframe['Month']=dataframe['DATE'].dt.month
        columns=dataframe.columns
        monthly_params=[]
        actual_mon=[]
        for col in columns:
            if 'Monthly' in col:
                actual_mon.append(col)
                param=col.replace('Monthly','')
                if 'WetBulb' not in param and 'Departure' not in param:
                    param=param.replace('Temperature','DryBulbTemperature')
                monthly_params.append(param)
        df_monthlt=dataframe.dropna(how='all',subset=actual_mon)[['Month']+actual_mon]
        output_file_path=os.path.join(out,file_name).replace('.csv','_prepare.csv')
        df_monthlt.to_csv(output_file_path,index=False)
        text_file_path=os.path.join(out,file_name.replace('.csv','.txt'))
        with open(text_file_path,'w') as f:
            f.write(','.join(monthly_params))
def main():
    params=yaml.safe_load(open("params.yaml"))
    link=params["data_source"]["temp_dir"]
    out=params["data_prepare"]["dest_folder"]
    os.makedirs(out, exist_ok=True)
    prepare_pred(link,out)
if __name__ == "__main__":
    main()

