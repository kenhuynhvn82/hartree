import pandas as pd
import numpy as np

def merge_two_datasets_by_pandas(input_file1: str, input_file2: str, output_file: str):
    df1 = pd.read_csv(input_file1)
    df2 = pd.read_csv(input_file2)
    df = pd.merge(df1, df2, how='inner', on='counter_party')
    
    gf = df.assign(
        status_ARAP=np.where(df['status'] == 'ARAP', df['value'], 0), 
        status_ACCR=np.where(df['status'] == 'ACCR', df['value'], 0)
    )
    
    all_group_columns = ['legal_entity', 'counter_party', 'tier']
    gf2 = gf.groupby(all_group_columns, as_index=False).agg({'rating':max, 'status_ARAP':sum, 'status_ACCR':sum})
    final = pd.DataFrame(gf2)
    for attr in [['legal_entity'], ['legal_entity', 'counter_party'], ['counter_party'], ['tier']]:
        attr_set = set(attr)
        extra_attr = [x for x in all_group_columns if x not in attr_set]
        dfByAttr = gf.groupby(attr, as_index=False).agg({'rating':max, 'status_ARAP':sum, 'status_ACCR':sum}).assign(**dict.fromkeys(extra_attr, 'Total'))
        final = pd.concat([final, dfByAttr])
    
    final.to_csv(output_file, index=False)
    