import pandas as pd
from sklearn.metrics import r2_score
import os
import yaml
def r2_gen(ground_truth,predicted,out_file):
    csv_files_1=[file for file in os.listdir(ground_truth) if file.endswith('.csv')]
    for file_name in csv_files_1:
        input_file_path_1=os.path.join(ground_truth,file_name) #Get the input files here
        input_file_path_2=os.path.join(predicted,file_name).replace('_prepare.csv','_process.csv')
        dataframe_1=pd.read_csv(input_file_path_1).dropna(axis=1,how='all') #drop the columns having Nan values
        dataframe_2=pd.read_csv(input_file_path_2).dropna(axis=1,how='all')
        common_columns = set(dataframe_1.columns).intersection(dataframe_2.columns) #See which columns are common in the aggregation and ground truth dataset
        dataframe_1 = dataframe_1.dropna(subset=common_columns) #Remove rows from the data frame where the common columns have Nan values
        dataframe_2 = dataframe_2.dropna(subset=common_columns)
        common_months = set(dataframe_1['Month']).intersection(dataframe_2['Month']) #Get the common months for which rewading exists in both the dataframes
        dataframe_1=dataframe_1[dataframe_1['Month'].isin(common_months)]
        dataframe_2=dataframe_2[dataframe_2['Month'].isin(common_months)]
        r2_scores=[]
        for col in common_columns:
            r2_scores.append(r2_score(dataframe_1[col],dataframe_2[col])) #Calculate the r2 score for each column
        output_r2='Consistent' #Default value
        for scores in r2_scores:
            if scores<0.9: #If even 1 score is less than 0.9 return inconsistent
                output_r2='Inconsistent'
                break
        print(output_r2) #Print this output
        output_file_path = os.path.join(out_file.rstrip(os.path.sep), file_name.replace('_prepare.csv', '_r2.txt')) #Write it to a txt file with this link
        with open(output_file_path,'w') as f:
            f.write(output_r2)
            #Write down the r2 score array also
            f.write('\n')
            f.write(','.join([str(score) for score in r2_scores]))
def main():
    params=yaml.safe_load(open("params.yaml")) #Read the yaml file and get the hyperlinks
    link_GT=params["data_prepare"]["dest_folder"]
    link_pred=params["data_process"]["dest_folder"]
    out_file=params["evaluate"]["output"]
    os.makedirs(out_file, exist_ok=True)
    r2_gen(link_GT,link_pred,out_file)
if __name__ == "__main__":
    main()