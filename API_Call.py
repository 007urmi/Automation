import requests
import pandas as pd
from csv import DictReader
import time
import os
import glob
from func_timeout import func_timeout, FunctionTimedOut
import pdb
import logging
#pdb.set_trace()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

file_name = str(input("Enter the file name with extension:"))
name = file_name.split('.')
if name[1] == 'csv':
    Underserved_folder_path = 'C://Users//urmimala.poddar//Desktop//KY_Wireless CBs_7 Counties//{}.csv'.format(name[0])
    Underserved_data = pd.read_csv(Underserved_folder_path)
else:
    Underserved_folder_path = 'C://Users//urmimala.poddar//Desktop//{}.xlsx'.format(name[0])
    Underserved_data = pd.read_excel(Underserved_folder_path)

#Underserved_data["GEOID10"] = Underserved_data["GEOID10"].values.astype(int)
Underserved_data["GEOID10"] = Underserved_data["GEOID10"].values.astype(str)
CountyFips = Underserved_data["GEOID10"].str[:5]
count = 1
total_api_calls = len(Underserved_data)
api_time_out_secs = 60
api_limit = 10
inactive = []
for cb in Underserved_data["GEOID10"]:
    parameters = {"cb": cb, "token": "PRRIUVVWACNQYHX", "fname": cb + ".csv"}

    csv_path = "C://Users//urmimala.poddar//Documents//CQA Data//Automated//KY_McCracken_test//{}{}".format(parameters["cb"], ".csv")

    # url = 'https://apps.costquest.com/api/fabriccsv?cb=171279701001108&token=PRRIUVVWACNQYHX&filename=171279701001108.csv'

    f = requests.get("https://apps.costquest.com/api/fabriccsv", params=parameters).content.decode('utf-8')
    reader = DictReader(f.split('\n'))
    csv_dict_list = list(reader)
    data = pd.DataFrame.from_dict(csv_dict_list, orient='columns')
    data.to_csv(csv_path)
    if data.empty:
        logging.info("Inactive CB:{}".format(parameters['cb']))
        inactive.append(parameters['cb'])

    for i in range(total_api_calls):
        if count == api_limit:
            logging.info('Downloaded 10CBs Fabric Data, Please wait')
            time.sleep(api_time_out_secs)
            api_limit = api_limit + 10

    print('Downloaded data for {}'.format(parameters['cb']))
    count += 1

