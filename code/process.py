import pandas as pd
import yaml
import os
def aggregate(input_link,dest_link,link_cols):
    #dataframe=pd.read_csv(input_link)
    csv_files=[file for file in os.listdir(input_link) if file.endswith('.csv')]
    for file_name in csv_files:
        input_file_path=os.path.join(input_link,file_name)
        dataframe=pd.read_csv(input_file_path)
        dataframe['DATE']=pd.to_datetime(dataframe['DATE'])
        dataframe['Month']=dataframe['DATE'].dt.month
        columns=dataframe.columns
        daily_params=[]
        actual_daily=[]
        cols_toberetained=[]
        actual_mon=[]
        new_columns=[]
        for col in columns:
            if 'Daily' in col:
                actual_daily.append(col)
                param=col.replace('Daily','')
                daily_params.append(param)
            if 'Monthly' in col:
                actual_mon.append(col)
        tfile_link=os.path.join(link_cols,file_name.replace('.csv','.txt'))
        with open(tfile_link,'r') as f:
            monthly_params=f.read().split(',')
        for i in range(len(daily_params)):
            for j in range(len(monthly_params)):
                if (monthly_params[j] in daily_params[i]) or ('Average' in daily_params[i] and (monthly_params[j].replace('Mean','').replace('Average','') in daily_params[i].replace('Average',''))):
                    cols_toberetained.append(actual_daily[i])
                    new_columns.append(actual_mon[j])
        #daily_columns=['DailyDepartureFromNormalAverageTemperature','DailyAverageDryBulbTemperature','DailyMaximumDryBulbTemperature','DailyMinimumDryBulbTemperature','DailyPrecipitation']
        df_date=dataframe.dropna(how='all',subset=cols_toberetained)[['Month']+cols_toberetained]
#Now aggregate by month and take average of other columns
        dtype_dict = {col: float for col in df_date.columns if col != 'Month'}
        df_date = df_date.astype(dtype_dict)
        #new_columns=['MonthlyDepartureFromNormalAverageTemperature','MonthlyAverageDryBulbTemperature','MonthlyMaximumDryBulbTemperature','MonthlyMinimumDryBulbTemperature','MonthlyPrecipitation']
        df_aggregated=df_date.groupby('Month').mean().reset_index().rename(dict(zip(cols_toberetained,new_columns)),axis=1)
#Convert aggregated to csv file 
        output_file_path=os.path.join(dest_link,file_name).replace('.csv','_process.csv')
        df_aggregated.to_csv(output_file_path,index=True)
def main():
    params=yaml.safe_load(open("params.yaml"))
    input_link=params["data_source"]["temp_dir"]
    link_cols=params["data_prepare"]["dest_folder"]
    dest_link=params["data_process"]["dest_folder"]
    os.makedirs(dest_link, exist_ok=True)
    aggregate(input_link,dest_link,link_cols)
if __name__ == "__main__":
    main()