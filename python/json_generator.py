import json
import pandas as pd

inpath = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/test_file.xlsx"
outpath = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/json_control_files/sample/"

df_excel = pd.read_excel(inpath)

raw_table_name = df_excel.raw_table_name.unique()

for i in range(len(raw_table_name)):
    # dataframe at table level
    df = df_excel.loc[df_excel['raw_table_name'] == raw_table_name[i]]

    dict = {}
    dict['application'] = ''.join([str(col) for col in df.application.unique()])
    dict['domain'] = ''.join([str(col) for col in df.domain.unique()])
    dict['entity_name'] = ''.join([str(col) for col in df.raw_table_name.unique()])
    dict['configurations'] = {
        'database': ''.join([str(col) for col in df.database.unique()]),
        'schema': ''.join([str(col) for col in df.schema.unique()]),
        'column_list': ','.join([str(col) for col in df.raw_column_name])
    }

    j = json.dumps(dict)
    with open(outpath + "{raw_table_name}.json".format(raw_table_name=raw_table_name[i]), 'w') as f:
        f.write(j)
