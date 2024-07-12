import pandas as pd


run_temp_bulk = pd.read_excel(r'D:\study\work_code\WEB\other\run_bulk.xlsx') # 去重，用作最终bulk file
run_group_bulk = run_temp_bulk.copy() # 聚合新值

hit_temp_bulk = pd.read_excel(r'D:\study\work_code\WEB\other\hit_bulk.xlsx') # 去重，用作最终bulk file
hit_group_bulk = hit_temp_bulk.copy() # 聚合新值

group_bulk = pd.read_excel(r'D:\study\work_code\WEB\other\group_bulk.xlsx')
group_bulk['Outlet ID'] = group_bulk['Outlet ID'].astype(str)
group_bulk['ITEM ID'] = group_bulk['ITEM ID'].astype(str)
group_bulk['id'] = group_bulk['Outlet ID'] + '-' + group_bulk['ITEM ID']

def virtual_bulk_input_run(run_temp_bulk, run_group_bulk):
    run_temp_bulk = run_temp_bulk.drop(columns=['Current Sales Units CP (E,C)','New Sales Units CP (E,C)'])
    run_temp_bulk = run_temp_bulk.drop_duplicates()
    run_temp_bulk['Outlet ID'] = run_temp_bulk['Outlet ID'].astype(str)
    run_temp_bulk['ITEM ID'] = run_temp_bulk['ITEM ID'].astype(str)
    run_temp_bulk['id'] = run_temp_bulk['Outlet ID'] + '-' + run_temp_bulk['ITEM ID']


    run_group_bulk['Outlet ID'] = run_group_bulk['Outlet ID'].astype(str)
    run_group_bulk['ITEM ID'] = run_group_bulk['ITEM ID'].astype(str)
    run_group_bulk['id'] = run_group_bulk['Outlet ID'] + '-' + run_group_bulk['ITEM ID']
    run_group_bulk = run_group_bulk.groupby(['id'], as_index=False).agg({
        'Current Sales Units CP (E,C)': 'sum',
        'New Sales Units CP (E,C)': 'sum'})
    run = pd.merge(run_temp_bulk, run_group_bulk, on=['id'], how='left')
    return run

def virtual_bulk_input_hit(hit_temp_bulk, hit_group_bulk):
    hit_temp_bulk = hit_temp_bulk.drop(columns=['Current Sales Units CP (E,C)','New Sales Units CP (E,C)'])
    hit_temp_bulk = hit_temp_bulk.drop_duplicates()
    hit_temp_bulk['Outlet ID'] = hit_temp_bulk['Outlet ID'].astype(str)
    hit_temp_bulk['ITEM ID'] = hit_temp_bulk['ITEM ID'].astype(str)
    hit_temp_bulk['id'] = hit_temp_bulk['Outlet ID'] + '-' + hit_temp_bulk['ITEM ID']

    hit_group_bulk['Outlet ID'] = hit_group_bulk['Outlet ID'].astype(str)
    hit_group_bulk['ITEM ID'] = hit_group_bulk['ITEM ID'].astype(str)
    hit_group_bulk['id'] = hit_group_bulk['Outlet ID'] + '-' + hit_group_bulk['ITEM ID']
    hit_group_bulk = hit_group_bulk.groupby(['id'], as_index=False).agg({
        'Current Sales Units CP (E,C)': 'sum',
        'New Sales Units CP (E,C)': 'sum'})
    hit = pd.merge(hit_temp_bulk, hit_group_bulk, on=['id'], how='left')
    return hit

def sol(run, hit, group_bulk):
    # 合并前两张表
    df_run_hit = pd.merge(run, hit, on='id', how='outer', suffixes=('_run', '_hit'))
    # 为 group_bulk 表的列添加 _group 后缀
    group_bulk = group_bulk.add_suffix('_group')
    group_bulk = group_bulk.rename(columns={'id_group': 'id'})  # 保留 'id' 列名不变
    # 合并所有三张表
    df_run_hit_group = pd.merge(df_run_hit, group_bulk, on='id', how='outer')
    # 填充基本信息字段
    basic_info_fields = [
        'QC ID', 'PG ID', 'CITY2', 'CITY2 ID', 'CountryChannel', 'CountryChannel ID',
        'Outlet ID', 'ORGANISAT TYPE', 'ORGANISAT TYPE ID', 'COPIES', 'COPIES ID',
        'BRAND', 'BRAND ID', 'ITEM', 'ITEM ID']
    for field in basic_info_fields:
        run_field = field + '_run'
        hit_field = field + '_hit'
        group_field = field + '_group'

        df_run_hit_group[run_field].fillna(df_run_hit_group[hit_field], inplace=True)
        df_run_hit_group[run_field].fillna(df_run_hit_group[group_field], inplace=True)

    # 填充 Current Sales Units CP (E,C)_run
    df_run_hit_group['Current Sales Units CP (E,C)_run'].fillna(
        df_run_hit_group['Current Sales Units CP (E,C)_hit'], inplace=True)
    df_run_hit_group['Current Sales Units CP (E,C)_run'].fillna(
        df_run_hit_group['Current Sales Units CP (E,C)_group'], inplace=True)

    # 填充 New Sales Units CP (E,C)_run
    df_run_hit_group['New Sales Units CP (E,C)_run'] = df_run_hit_group.apply(
        lambda row: row['New Sales Units CP (E,C)_group'] if pd.notna(row['New Sales Units CP (E,C)_group']) else
        (row['New Sales Units CP (E,C)_hit'] if pd.notna(row['New Sales Units CP (E,C)_hit']) else
         row['New Sales Units CP (E,C)_run']),axis=1)

    # 只保留第一张表中的字段，并移除'_run'后缀
    columns_to_keep = [col for col in df_run_hit_group.columns if col.endswith('_run')]
    columns_to_keep = [col.replace('_run', '') for col in columns_to_keep]
    df_final = df_run_hit_group[[col + '_run' for col in columns_to_keep]]
    df_final.columns = columns_to_keep
    df_final.to_excel(r'D:\study\work_code\WEB\other\bulk.xlsx', index=False)

run = virtual_bulk_input_run(run_temp_bulk, run_group_bulk)
hit = virtual_bulk_input_run(hit_temp_bulk, hit_group_bulk)
sol(run,hit,group_bulk)

