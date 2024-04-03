import pandas as pd
import yaml
import os
def aggregate(input_link,dest_link,link_cols):
    #dataframe=pd.read_csv(input_link)
    csv_files=[file for file in os.listdir(input_link) if file.endswith('.csv')] #We get all the csv files
    for file_name in csv_files:
        input_file_path=os.path.join(input_link,file_name) #We get the input file path
        dataframe=pd.read_csv(input_file_path)
        dataframe['DATE']=pd.to_datetime(dataframe['DATE']) #Convert the date to datetime type
        dataframe['Month']=dataframe['DATE'].dt.month #Get the month in the data frame
        columns=dataframe.columns
        daily_params=[]
        actual_daily=[]
        cols_toberetained=[]
        actual_mon=[]
        new_columns=[]
        for col in columns:
            if 'Daily' in col: #We get the columns with Daily in them
                actual_daily.append(col)
                param=col.replace('Daily','')
                daily_params.append(param)
            if 'Monthly' in col: #We get the columns with Monthly in them
                actual_mon.append(col)
        tfile_link=os.path.join(link_cols,file_name.replace('.csv','.txt'))    #Open the text file created in prepare.py
        with open(tfile_link,'r') as f:
            monthly_params=f.read().split(',')
        for i in range(len(daily_params)):
            for j in range(len(monthly_params)): #Here we consider the common columns, some have mean instead of averagein monthly hence we use the if condition
                if (monthly_params[j] in daily_params[i]) or ('Average' in daily_params[i] and (monthly_params[j].replace('Mean','').replace('Average','') in daily_params[i].replace('Average',''))):
                    cols_toberetained.append(actual_daily[i])
                    new_columns.append(actual_mon[j])
        df_date=dataframe.dropna(how='all',subset=cols_toberetained)[['Month']+cols_toberetained] #drop all rows where all values are missing
#Now aggregate by month and take average of other columns
        dtype_dict = {col: float for col in df_date.columns if col != 'Month'}
        df_date = df_date.astype(dtype_dict)
        df_aggregated=df_date.groupby('Month').mean().reset_index().rename(dict(zip(cols_toberetained,new_columns)),axis=1) #Do aggregation,use mean and reset index. Rename the columns to ones having monthly
#Convert aggregated to csv file 
        output_file_path=os.path.join(dest_link,file_name).replace('.csv','_process.csv')
        df_aggregated.to_csv(output_file_path,index=True)
def main():
    params=yaml.safe_load(open("params.yaml")) #Get the parameters
    input_link=params["data_source"]["temp_dir"]
    link_cols=params["data_prepare"]["dest_folder"]
    dest_link=params["data_process"]["dest_folder"]
    os.makedirs(dest_link, exist_ok=True) #Create the output folder if it doesnt exist
    aggregate(input_link,dest_link,link_cols)
if __name__ == "__main__": #Execute the main function
    main()