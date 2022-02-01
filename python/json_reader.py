import json
import pandas as pd

with open('D:/datasets/Cloud_Issuance.json') as data_file:
    data = json.load(data_file)
	policyLobList = data['PolicyLobList']
	
#selecting only nested columns in JSON file
nested_df = pd.json_normalize(data, 'PolicyCustomerList', 'PolicyLobList')
nested_df.to_csv('D:/datasets/output/nested_columns.csv')

#selecting columns from nested_columns in JSON file
policyRiskList_df = pd.json_normalize(policyLobList, 'PolicyRiskList')
policyRiskList_df.to_csv('D:/datasets/output/PolicyRiskList_columns.csv')

df = pd.DataFrame(data)

#dataFrame without "PolicyCustomerList", "PolicyLobList"
df1 = df.drop(['PolicyCustomerList', 'PolicyLobList'], axis= 1)

#dataFrame without "PolicyCustomerList"
df2 = df.drop(['PolicyCustomerList'], axis= 1)

#dataFrame without "PolicyLobList"
df3 = df.drop(['PolicyLobList'], axis= 1)

df1.to_csv('D:/datasets/output/output1.csv')
df2.to_csv('D:/datasets/output/output2.csv')
df3.to_csv('D:/datasets/output/output3.csv')

#dataFrame with all the columns
df.to_csv('D:/datasets/output/output4.csv')
