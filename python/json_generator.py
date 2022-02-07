import json
import pandas as pd


# function to read data dictionary
def readDataDict(source_path):
    df = pd.read_excel(source_path)
    return df


# function to write json files
def writeJson(df, dest_path, entity_name=None):
    raw_table_name = df.raw_table_name.unique()

    # write json file for a specific entity
    if entity_name in raw_table_name:
        df_table = df.loc[df['raw_table_name'] == entity_name]
        d = {'application': ''.join([str(col) for col in df_table.application.unique()]),
             'domain': ''.join([str(col) for col in df_table.domain.unique()]),
             'entity_name': ''.join([str(col) for col in df_table.raw_table_name.unique()]),
             'configurations': {
                 'database': ''.join([str(col) for col in df_table.database.unique()]),
                 'schema': ''.join([str(col) for col in df_table.schema.unique()]),
                 'column_list': ','.join([str(col) for col in df_table.raw_column_name])
             }}
        a = json.dumps(d)
        with open(dest_path + "table_config.json", 'w') as f:
            f.write(a)

        # drop entity from dataframe
        index_name = df[df['raw_table_name'] == entity_name].index
        df.drop(index_name, inplace=True)
        raw_table_name = df.raw_table_name.unique()
    else:
        pass

    # write json file for the remaining or all entities
    json_list = []
    for i in range(len(raw_table_name)):
        # dataframe at entity level
        df_json = df.loc[df['raw_table_name'] == raw_table_name[i]]
        d = {'application': ''.join([str(col) for col in df_json.application.unique()]),
             'domain': ''.join([str(col) for col in df_json.domain.unique()]),
             'entity_name': ''.join([str(col) for col in df_json.raw_table_name.unique()]),
             'configurations': {
                 'database': ''.join([str(col) for col in df_json.database.unique()]),
                 'schema': ''.join([str(col) for col in df_json.schema.unique()]),
                 'column_list': ','.join([str(col) for col in df_json.raw_column_name])
             }}
        json_list.append(d)
    j = json.dumps(json_list)
    with open(dest_path + "test_config.json", 'w') as f:
        f.write(j)


if __name__ == '__main__':
    source_path = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/test_file.xlsx"
    dest_path = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/json_control_files/sample/"

    df = readDataDict(source_path)
    writeJson(df, dest_path, entity_name='accounts')
