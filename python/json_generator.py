import json
import pandas as pd

inpath = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/test_file.xlsx"
outpath = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/json_control_files/"

df_excel = pd.read_excel(inpath)

database = df_excel.database.unique()

for i in range(len(database)):
    # dataframe at database level
    df_database = df_excel.loc[df_excel['database'] == database[i]]
    raw_table_name = df_database.raw_table_name.unique()

    for y in range(len(raw_table_name)):
        # dataframe at table level
        df_table = df_database.loc[df_database['raw_table_name'] == raw_table_name[y]]

        dict = {}
        dict['application'] = df_table.application.unique().tolist()  # convert np.array to list to make it json serializable
        dict['database'] = df_table.database.unique().tolist()
        dict['schema'] = df_table.schema.unique().tolist()
        dict['domain'] = df_table.domain.unique().tolist()
        dict['raw_table_name'] = df_table.raw_table_name.unique().tolist()

        column = []
        for col in df_table.raw_column_name:
            column.append(col)

        dict['raw_column_name'] = column

        j = json.dumps(dict)
        with open(outpath + "{raw_table_name}.json".format(raw_table_name=raw_table_name[i]), 'w') as f:
            f.write(j)
