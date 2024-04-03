import pandas as pd
import yaml
import os
def prepare_pred(link,out):
    csv_files=[file for file in os.listdir(link) if file.endswith('.csv')] #We get all the csv files
    for file_name in csv_files:
        input_file_path=os.path.join(link,file_name)
        dataframe=pd.read_csv(input_file_path)
        dataframe['DATE']=pd.to_datetime(dataframe['DATE'])
        dataframe['Month']=dataframe['DATE'].dt.month #Convert the date to datetime type and get the month in the data frame
        columns=dataframe.columns
        monthly_params=[]
        actual_mon=[]
        for col in columns:
            if 'Monthly' in col:
                actual_mon.append(col) #We get the columns with Monthly in them
                param=col.replace('Monthly','')
                if 'WetBulb' not in param and 'Departure' not in param: #We change name of columns having only temperature to DryBulbTemperature
                    param=param.replace('Temperature','DryBulbTemperature') #This is done as daily has DryBulb mentioned. In reality, it is the same as temperature
                monthly_params.append(param)
        df_monthlt=dataframe.dropna(how='all',subset=actual_mon)[['Month']+actual_mon] #Drop all rows where all values are missing
        output_file_path=os.path.join(out,file_name).replace('.csv','_prepare.csv')
        df_monthlt.to_csv(output_file_path,index=False) #Convert to csv file
        text_file_path=os.path.join(out,file_name.replace('.csv','.txt'))
        with open(text_file_path,'w') as f:
            f.write(','.join(monthly_params)) #Write the column names having monthly to a text file
def main():
    params=yaml.safe_load(open("params.yaml")) #Get parameters from the yaml file
    link=params["data_source"]["temp_dir"]
    out=params["data_prepare"]["dest_folder"]
    os.makedirs(out, exist_ok=True) #Create the output folder
    prepare_pred(link,out)
if __name__ == "__main__":
    main()

