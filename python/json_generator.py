import json
import pandas as pd

inpath = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/test_file.xlsx"
outpath = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/json_control_files/"

df_excel = pd.read_excel(inpath)

raw_table_name = df_excel.raw_table_name.unique()

for i in range(len(raw_table_name)):
    df = df_excel.loc[df_excel['raw_table_name'] == raw_table_name[i]]

    dict = {}
    dict['application'] = df.application.unique().tolist()  #convert np.array to list to make it json serializable
    dict['database'] = df.database.unique().tolist()
    dict['schema'] = df.schema.unique().tolist()
    dict['domain'] = df.domain.unique().tolist()
    dict['raw_table_name'] = df.raw_table_name.unique().tolist()

    column = []
    for col in df.raw_column_name:
        column.append(col)

    dict['raw_column_name'] = column

    j = json.dumps(dict)
    with open(outpath+"{raw_table_name}.json".format(raw_table_name=raw_table_name[i]), 'w') as f:
        f.write(j)