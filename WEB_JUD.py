import pandas as pd
import os
import sys
import numpy as np
import time
import subprocess

class RUN_city_by_region():
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.run_data = pd.read_excel('WEB Offline 100C MONTHLY_2405M.xlsx',header=18) #计算使用
        self.run_data_bulk_ori = self.run_data.copy()  # 生成bulk文件及模拟bulk进入系统使用

    def run_data_init(self):
        # 在这个函数中，我打算用来执行处理数据文件的功能，也就是说将原始数据预处理成为我想要的文件样式，在进行后续编写和处理，分层编写，逻辑更清晰
        print('Start refactoring run_data.')
        green_text = '\033[92m'
        reset_text = '\033[0m'
        #筛选稳定可比店
        self.run_data = self.run_data.fillna(0)
        self.run_data_0 = self.run_data[self.run_data['COPIES'] == 'REGULAR']
        self.run_data_0 = pd.pivot_table(self.run_data_0,index=['Outlet'],
                                         values=['Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)'],
                                         aggfunc='sum')
        self.run_data_0 = self.run_data_0.loc[
            (self.run_data_0['Sales Units R1 (NE,NC)'] != 0) & (self.run_data_0['Sales Units CP (NE,NC)'] != 0)]
        self.run_data_0.to_excel(r'Result_run/useful_outlet.xlsx', index=True)
        self.run_data_0 = self.run_data_0.reset_index()
        not_in_run_data_0 = ~self.run_data['Outlet'].isin(self.run_data_0['Outlet'])
        self.run_data.loc[not_in_run_data_0,['Sales Units R1 (NE,NC)','Sales Units CP (NE,NC)']] = 0
        self.run_data.to_excel(r'Result_run/run_ori.xlsx', index=False)

        # 分组聚合
        self.run_data = pd.pivot_table(self.run_data,
                                       index=['REGION2', 'CITY2', 'BRAND'],
                                       values=['Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)',
                                               'Sales Units R1 (E,C)', 'Sales Units CP (E,C)'],
                                       aggfunc='sum')
        self.run_data.reset_index(inplace=True) #将行索引转换为列，这样能解决pivot造成的合并单元问题

        self.run_data['CITY3'] = self.run_data['REGION2'] + '-' + self.run_data['CITY2']

        new_order = ['REGION2', 'CITY2','CITY3', 'BRAND','Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)',
                                                'Sales Units R1 (E,C)', 'Sales Units CP (E,C)']
        self.run_data = self.run_data[new_order] #按开发需求顺序显示字段
        self.run_data.to_excel(r'Result_run/run_file_1.xlsx', index=False) #文件file_1为聚合后调整过格式后文件
        print(green_text +f'1. The run_file_1 has been saved in: {os.path.abspath("Result_run/run_file_1.xlsx")}'+ reset_text)

        # 开始计算各关键值，相当于文件重构，得到一个字段齐全的待解决文件
        # 1、按城市级别计算份额占比
        self.run_data['R1_NENC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units R1 (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['CP_NENC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units CP (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['R1_EC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units R1 (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['CP_EC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units CP (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['share_diff'] = self.run_data['CP_EC_share %'] - self.run_data['R1_EC_share %']

        # 2、环比计算(考虑到可能分母出现0，这里做了安全除法运算，但是在提需求的时候，这部分细节其实需要自己想好如何处理)
        self.run_data['NENC_M2M %'] = self.run_data.apply(
            lambda row: (row['Sales Units CP (NE,NC)'] / row['Sales Units R1 (NE,NC)'] - 1)
            if row['Sales Units R1 (NE,NC)'] != 0 else 0,axis=1)
        self.run_data['EC_M2M %'] = self.run_data.apply(
            lambda row: (row['Sales Units CP (E,C)'] / row['Sales Units R1 (E,C)'] - 1)
            if row['Sales Units R1 (E,C)'] != 0 else 0,axis=1)
        self.run_data.to_excel(r'Result_run/run_file_2.xlsx', index=False) #文件file_2为添加计算值后文件
        print(green_text +f'2. The run_file_2 has been saved in: {os.path.abspath("Result_run/run_file_2.xlsx")}'+ reset_text)

        # 3、逻辑判断值编写（这里是整个代码的核心所在，需要能枚举出所有需要的情况，提需求时需要格外注意）
        conditions = [(abs(self.run_data['share_diff']) >= 0.05),

                      ((abs(self.run_data['EC_M2M %']) >= 0.5) & (self.run_data['Sales Units CP (E,C)'] > 50)),

                      ((self.run_data['Sales Units CP (E,C)'] != 0) & (self.run_data['NENC_M2M %'] * self.run_data['EC_M2M %'] < 0) &
                       ((self.run_data['Sales Units R1 (NE,NC)'] > 50) & (self.run_data['Sales Units CP (NE,NC)'] > 50))),

                      ((self.run_data['Sales Units CP (E,C)'] != 0) & (self.run_data['NENC_M2M %'] * self.run_data['EC_M2M %'] < 0) &
                       ((self.run_data['Sales Units R1 (NE,NC)'] <= 50) & (self.run_data['Sales Units CP (NE,NC)'] <= 50)))]
        values = ['Share difference exceeds 5%, please check',
                  'EC month on month exceeds 50%, please check',
                  'Not trend V1, please check',
                  'Not trend V2, please check']
        self.run_data['Result'] = np.select(conditions, values, default='No action required')

        self.run_data.to_excel(r'Result_run/run_file_3.xlsx', index=False) #文件file_3添加逻辑判断后文件
        print(green_text +f'3. The run_file_3 has been saved in: {os.path.abspath("Result_run/run_file_3.xlsx")}'+ reset_text)
        print('Run_data reconstruction completed.\n')

    def run_data_func(self):
        # 在这个函数中，我用上一个函数执行完的数据集作基础，根据重构后的文件，解决后续问题，这样能使代码更有层次感，便于后期维护
        print('Start run_data logical processing of data')
        # start_time = time.time()
        green_text = '\033[92m'
        red_text = '\033[91m'
        reset_text = '\033[0m'

        self.run_data['Adj_CP_EC_share %'] = None #调整至份额占比
        self.run_data['Adj_CP_EC_M2M %'] = None #调整至环比
        self.run_data['New_CP_EC'] = None #新CP（E,C）值
        self.run_data['New_CP_EC_share %'] = None #新CP（E,C）值份额占比
        self.run_data['New_CP_EC_share_diff %'] = None #新CP（E,C）值份额占比差值
        self.run_data['New_CP_EC_M2M %'] = None #新CP（E,C）值环比
        self.run_data['Adj_value'] = None #最终调整值

        for index, row in self.run_data.iterrows():
            # 1、处理'Share difference exceeds 5%, please check'的情形
            if row['Result'] == condition_0:
                city_value = row['CITY3']
                city_group_sum = self.run_data[self.run_data['CITY3'] == city_value]['Sales Units CP (E,C)'].sum()
                # print(city_value, city_group_sum)
                # 这一步比较难理解,因为需要计算新CP（E,C）值，所以需要按照CITY聚合求出当前CP（E,C）在同一CITY维度下的总和
                if row['CP_EC_share %'] > row['R1_EC_share %']:
                    adj_ec_share = row['R1_EC_share %'] + 0.015
                    new_cp_ec = city_group_sum * adj_ec_share
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                    new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] -1
                else:
                    adj_ec_share = row['R1_EC_share %'] - 0.015
                    new_cp_ec = city_group_sum * adj_ec_share
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                    new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] -1
                # 更新DataFrame中的新字段值
                self.run_data.at[index, 'Adj_CP_EC_share %'] = adj_ec_share
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index,'Adj_value'] = adj_value
                self.run_data.at[index,'New_CP_EC_M2M %'] = new_cp_ec_m2m

        for index,row in self.run_data.iterrows():
            # 2、处理'EC month on month exceeds 50%, please check'的情形
            if row['Result'] == condition_1:
                if row['NENC_M2M %'] > 0:
                    adj_cp_ec_m2m = 0.22
                    new_cp_ec = 1.22 * row['Sales Units R1 (E,C)']
                    new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] -1
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                else:
                    adj_cp_ec_m2m = -0.22
                    new_cp_ec = 0.78 * row['Sales Units R1 (E,C)']
                    new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                self.run_data.at[index,'Adj_CP_EC_M2M %'] = adj_cp_ec_m2m
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m_ration
                self.run_data.at[index, 'Adj_value'] = adj_value

        for index,row in self.run_data.iterrows():
            # 3、处理'Not trend, please check'的情形
            if row['Result'] == condition_2:
                adj_cp_ec_m2m = row['NENC_M2M %'] * 0.6
                mid = 1 + adj_cp_ec_m2m
                new_cp_ec = row['Sales Units R1 (E,C)'] * mid
                new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] -1
                adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                self.run_data.at[index, 'Adj_CP_EC_M2M %'] = adj_cp_ec_m2m
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m
                self.run_data.at[index, 'Adj_value'] = adj_value

            if row['Result'] == condition_3:
                if row['NENC_M2M %'] > 0:
                    adj_cp_ec_m2m = 0.02
                    mid = 1 + adj_cp_ec_m2m
                    new_cp_ec = row['Sales Units R1 (E,C)'] * mid
                    new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                else:
                    adj_cp_ec_m2m = -0.02
                    mid = 1 + adj_cp_ec_m2m
                    new_cp_ec = row['Sales Units R1 (E,C)'] * mid
                    new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                self.run_data.at[index, 'Adj_CP_EC_M2M %'] = adj_cp_ec_m2m
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m
                self.run_data.at[index, 'Adj_value'] = adj_value

        for index,row in self.run_data.iterrows():
            # 4、处理新CP（E,C）值导致的动态city.sum问题,处理思路是先把异常点计算出，最后统一更新
            if pd.isna(row['New_CP_EC']):
                new_cp_ec = row['Sales Units CP (E,C)']
                self.run_data.at[index,'New_CP_EC'] = new_cp_ec

        for index,row in self.run_data.iterrows():
            # 5、最后计算因容量动态变化需更新'New_CP_EC_share %','New_CP_EC_share_diff %'两个值
            condition = {condition_0, condition_1, condition_2, condition_3}
            if row['Result'] in condition:
                city_value = row['CITY3']
                city_group_sum = self.run_data[self.run_data['CITY3'] == city_value]['New_CP_EC'].sum()
                new_cp_ec_share = row['New_CP_EC'] / city_group_sum if city_group_sum != 0 else 0
                nwe_cp_ec_share_diff_ratio = new_cp_ec_share - row['R1_EC_share %']
                self.run_data.at[index,'New_CP_EC_share %'] = new_cp_ec_share
                self.run_data.at[index,'New_CP_EC_share_diff %'] = nwe_cp_ec_share_diff_ratio
        self.run_data.to_excel(r'Result_run/run_result_manual.xlsx', index=False)
        print(green_text +f'4. The run_result_file has been saved in: {os.path.abspath("Result_run/run_result_manual.xlsx")}'+ reset_text)
        input(red_text + 'Please check and change the run_result_manual file by manual,press "Enter" to continue working.' + reset_text)
        self.run_data = pd.read_excel(r'Result_run/run_result_manual.xlsx')
        print('Run_data logic processing completed\n')
        # 如果当判异程序使用的话运行至此即可

    def run_data_bulk(self):
        print('Starting to generate run_data bulk file, please wait')
        green_text = '\033[92m'
        reset_text = '\033[0m'
        # 该函数用来生成并控制bulk文件，会考虑效率的问题
        bulk_range = ['CITY3','BRAND','Sales Units CP (E,C)','New_CP_EC','Adj_value']
        bulk_select_data = self.run_data[bulk_range].copy()
        bulk_select_data = bulk_select_data[pd.notna(bulk_select_data['Adj_value'])]
        bulk_select_data['Run_key_id'] = bulk_select_data['CITY3'] + '-' + bulk_select_data['BRAND']

        #重命名，为了后边做映射区分字段用，逻辑上衔接不上，属于编写过程中思考和调试后加的代码
        bulk_select_data.rename(columns={'Sales Units CP (E,C)':'Ori_Sales Units CP (E,C)'},inplace=True)

        new_order = ['CITY3','BRAND','Run_key_id','Ori_Sales Units CP (E,C)','New_CP_EC','Adj_value']
        bulk_select_data = bulk_select_data[new_order]
        bulk_select_data.to_excel(r'Result_run/run_data_bulk_range.xlsx', index=False)

        print(green_text +f'5. Run_data bulk file modification scope has been saved in: '
                          f'{os.path.abspath("Result_run/bulk_range_data.xlsx")}'+ reset_text)

        self.run_data_bulk_ori['Run_key_id'] = (self.run_data_bulk_ori['REGION2'] + '-' + self.run_data_bulk_ori['CITY2']
                                                + '-' + self.run_data_bulk_ori['BRAND'])
        self.run_data_bulk_ori['table'] = np.where(self.run_data_bulk_ori['CountryChannel'].isin(
            ['CN - Computerhardware-Shops','CN - Mobile Phone Specialists']),'Y',np.nan)
        self.run_data_bulk_ori['Sta_adj'] = None
        for index,row in self.run_data_bulk_ori.iterrows():
            # 计算满足可调整量
            if row['table'] == 'Y':
                key_value = row['Run_key_id']
                brand_group_sum = self.run_data_bulk_ori[(self.run_data_bulk_ori['Run_key_id'] == key_value) &
                                                         (self.run_data_bulk_ori['table'] == 'Y')]['Sales Units CP (E,C)'].sum()
                self.run_data_bulk_ori.at[index,'Sta_adj'] = brand_group_sum
        # 映射,原理很简单相当于Vlookup函数的功能，代码逻辑较难理解
        fields_to_map = ['Ori_Sales Units CP (E,C)', 'New_CP_EC', 'Adj_value']
        map_data = bulk_select_data[['Run_key_id'] + fields_to_map] #映射表
        for index, row in self.run_data_bulk_ori.iterrows():
            if row['table'] == 'Y':
                key_value = row['Run_key_id'] # 获取当前行的Run_key_id值
                # 查找映射表中对应的行
                matching_row = map_data[map_data['Run_key_id'] == key_value]
                #在映射表表中过滤出Run_key_id等于key_value的行，如果找到了匹配的行，matching_row将包含这些行数据，此时的matching_row是Dataframe
                if not matching_row.empty: #若matching_row不为空，表示有匹配的数据被找到
                    for field in fields_to_map: #接着在fields_to_map列表中遍历需要map的字段
                        self.run_data_bulk_ori.at[index, field] = matching_row[field].values[0]

        #计算最终bulk结果，并加入代码容错判断，以提高容错率
        printed_ids = set()  # 用于跟踪已经打印过警告消息的 Run_key_id,利用的原理是集合的唯一性，只会唯一保存一个Run_key_id
        self.run_data_bulk_ori['Mid_value'] = None
        self.run_data_bulk_ori['K'] = None
        self.run_data_bulk_ori['Bulk_value'] = None
        for index, row in self.run_data_bulk_ori.iterrows():
            #该处判断较多，逻辑为：先检测非空条目->判断负值是否够减 最后再逐层展开
            if pd.notna(row['Adj_value']) and pd.notna(row['Sta_adj']):
                if row['Adj_value'] < 0:
                    if row['Sta_adj'] < abs(row['Adj_value']):
                        run_key_id = row['Run_key_id']
                        if run_key_id not in printed_ids:
                            print(f"{run_key_id} can't be satisfied, please check!")
                            printed_ids.add(run_key_id)
                            input('Attention! This is an error for robustness handling.')
                            input('(1) Please first record the current error ID and terminate the program;')
                            input('(2) Based on the run_result_manual file and the original file, manually '
                                  'note the new values that need to be modified for the "Adj_value" and "New_CP_EC" '
                                  'fields in the run_result_manual file;')
                            input('(3) Re-run the program, and when manually reviewing the run_result_manual file '
                                  'during the second run, be sure to modify the values noted earlier, save the file, '
                                  'and press Enter to continue executing the program.')
                    else:
                        mid_value = row['Sta_adj'] + row['Adj_value']
                        k = mid_value / row['Sta_adj']
                        bulk_value = row['Sales Units CP (E,C)'] * k
                        self.run_data_bulk_ori.at[index,'Mid_value'] = mid_value
                        self.run_data_bulk_ori.at[index,'K'] = k
                        self.run_data_bulk_ori.at[index,'Bulk_value'] = bulk_value
                else:
                    mid_value = row['Sta_adj'] + row['Adj_value']
                    k = mid_value / row['Sta_adj']
                    bulk_value = row['Sales Units CP (E,C)'] * k
                    self.run_data_bulk_ori.at[index, 'Mid_value'] = mid_value
                    self.run_data_bulk_ori.at[index, 'K'] = k
                    self.run_data_bulk_ori.at[index, 'Bulk_value'] = bulk_value

        if not printed_ids:
            print("Congratulations! No data that cannot be operated was found in actionable detection.")

        self.run_data_bulk_ori.to_excel(r'Result_run/bulk_process.xlsx', index=False)
        print(green_text + f'6. The bulk_process_file has been saved in: '
                           f'{os.path.abspath("Result_run/bulk_process.xlsx")}' + reset_text)
        temp = self.run_data_bulk_ori.copy()
        #还原
        mask = (pd.notna(self.run_data_bulk_ori['Bulk_value'])) & (self.run_data_bulk_ori['Bulk_value'] != 0)
        # 确保 `Bulk_value` 列中的数据类型与 `Sales Units CP (E,C)` 列兼容
        self.run_data_bulk_ori.loc[mask, 'Sales Units CP (E,C)'] = self.run_data_bulk_ori.loc[
            mask, 'Bulk_value'].astype('float64')
        self.run_data_bulk_ori.drop(columns=[
            'Sta_adj','Ori_Sales Units CP (E,C)','New_CP_EC','Adj_value','Mid_value',
            'K','Bulk_value'], inplace=True)
        self.run_data_bulk_ori.to_excel(r'HITLIST_KEY_brand_item_from_rawdata.xlsx',index=False)

        temp_bulk = temp[(pd.notna(temp['Bulk_value'])) &(temp['Bulk_value'] != 0)].copy()
        temp_bulk.loc[:, 'QC ID'] = 257951
        temp_bulk.loc[:, 'CITY2 ID'] = temp_bulk['CITY2 ID'].fillna(temp_bulk['CITY ID'])
        def virtual_bulk_input(temp_bulk):
            select_bulk_9 = ['QC ID', 'Productgroup', 'CITY2', 'CITY2 ID', 'CountryChannel', 'CountryChannel ID',
                           'Outlet ID', 'ORGANISAT TYPE','ORGANISAT TYPE ID', 'COPIES', 'COPIES ID',
                           'BRAND', 'BRAND ID', 'Item', 'Item ID', 'Sales Units CP (E,C)', 'Bulk_value']
            temp_bulk = temp_bulk[select_bulk_9]
            temp_bulk= temp_bulk.rename(columns={'Productgroup': 'PG ID','Item':'ITEM','Item ID':'ITEM ID',
                                                                            'Sales Units CP (E,C)': 'Current Sales Units CP (E,C)',
                                                                            'Bulk_value': 'New Sales Units CP (E,C)'},inplace=False)
            temp_bulk.to_excel(r'other/run_bulk.xlsx', index=False)
            print(green_text + f'7. The run_bulk file has been saved in: '
                               f'{os.path.abspath("other/run_bulk.xlsx")}' + reset_text)
            return temp_bulk
        virtual_bulk_input(temp_bulk) # 若后期不需要检查了可以注释此句代码
        print('The Run_process has already completed.')


class HITLIST_KEY_brand():
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.hit_data = pd.read_excel('HITLIST_KEY_brand_item_from_rawdata.xlsx')
        self.hit_data_bulk_ori = self.hit_data.copy()
        self.useful = pd.read_excel(r'Result_run/useful_outlet.xlsx') # 保持前后步骤可比店是一致的

    def hit_data_init(self):
        print('Start refactoring hit_data.')
        orange_text = '\033[93m'
        reset_text = '\033[0m'
        key_brand = ['APPLE','HUAWEI','HONOR','LENOVO','OPPO','VIVO','XIAOMI','REDMI','SAMSUNG']
        not_in_useful = ~self.hit_data['Outlet'].isin(self.useful['Outlet'])
        self.hit_data.loc[not_in_useful & (self.hit_data['COPIES'] != 'REGULAR'),
        ['Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)']] = 0
        self.hit_data = self.hit_data[self.hit_data['BRAND'].isin(key_brand)]
        self.hit_data.to_excel(r'Result_hit/hit_file_0.xlsx',index=False)
        print(orange_text +f'1. The hit_file_0 has been saved in: {os.path.abspath("Result_hit/hit_file_0.xlsx")}'+ reset_text)
        # 分组聚合
        self.hit_data = pd.pivot_table(self.hit_data,
                                       index=['BRAND','Item'],
                                       values=['Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)',
                                               'Sales Units R1 (E,C)', 'Sales Units CP (E,C)'],
                                       aggfunc='sum')
        self.hit_data.reset_index(inplace=True)
        new_order = ['BRAND','Item','Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)',
                     'Sales Units R1 (E,C)', 'Sales Units CP (E,C)']
        self.hit_data = self.hit_data[new_order]
        self.hit_data.to_excel(r'Result_hit/hit_file_1.xlsx', index=False)
        print(orange_text +f'2. The hit_file_1 has been saved in: {os.path.abspath("Result_hit/hit_file_1.xlsx")}'+ reset_text)

        self.hit_data['R1_NENC_share %'] = self.hit_data.groupby('BRAND')[
            'Sales Units R1 (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.hit_data['CP_NENC_share %'] = self.hit_data.groupby('BRAND')[
            'Sales Units CP (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.hit_data['R1_EC_share %'] = self.hit_data.groupby('BRAND')[
            'Sales Units R1 (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.hit_data['CP_EC_share %'] = self.hit_data.groupby('BRAND')[
            'Sales Units CP (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.hit_data['share_diff'] = self.hit_data['CP_EC_share %'] - self.hit_data['R1_EC_share %']

        self.hit_data['NENC_M2M %'] = self.hit_data.apply(
            lambda row: (row['Sales Units CP (NE,NC)'] / row['Sales Units R1 (NE,NC)'] - 1)
            if row['Sales Units R1 (NE,NC)'] != 0 else 0,axis=1)
        self.hit_data['EC_M2M %'] = self.hit_data.apply(
            lambda row: (row['Sales Units CP (E,C)'] / row['Sales Units R1 (E,C)'] - 1)
            if row['Sales Units R1 (E,C)'] != 0 else 0,axis=1)
        self.hit_data.to_excel(r'Result_hit/hit_file_2.xlsx', index=False)
        print(orange_text +f'3. The hit_file_2 has been saved in: {os.path.abspath("Result_hit/hit_file_2.xlsx")}'+ reset_text)

        conditions = [(abs(self.hit_data['share_diff']) >= 0.03),
                      ((self.hit_data['Sales Units CP (E,C)'] > 100) & (abs(self.hit_data['EC_M2M %']) >= 0.3))]
        values = ['Share difference exceeds 3%, please check',
                  'EC month on month exceeds 30%, please check']
        self.hit_data['Result'] = np.select(conditions, values, default='No action required')
        self.hit_data.to_excel(r'Result_hit/hit_file_3.xlsx', index=False)
        print(orange_text +f'4. The hit_file_3 has been saved in: {os.path.abspath("Result_hit/hit_file_3.xlsx")}'+ reset_text)
        print('Hit_data reconstruction completed.\n')

    def hit_data_func(self):
        print('Start hit_data logical processing of data')
        orange_text = '\033[93m'
        red_text = '\033[91m'
        reset_text = '\033[0m'
        global condition_4
        global condition_5
        self.hit_data['Adj_CP_EC_share %'] = None #调整至份额占比
        self.hit_data['Adj_CP_EC_M2M %'] = None #调整至环比
        self.hit_data['New_CP_EC'] = None #新CP（E,C）值
        self.hit_data['New_CP_EC_share %'] = None #新CP（E,C）值份额占比
        self.hit_data['New_CP_EC_share_diff %'] = None #新CP（E,C）值份额占比差值
        self.hit_data['New_CP_EC_M2M %'] = None #新CP（E,C）值环比
        self.hit_data['Adj_value'] = None #最终调整值

        for index, row in self.hit_data.iterrows():
            # 1、处理'Share difference exceeds 3%, please check'的情形
            if row['Result'] == condition_4:
                brand_value = row['BRAND']
                brand_group_sum = self.hit_data[self.hit_data['BRAND'] == brand_value]['Sales Units CP (E,C)'].sum()
                # print(brand_value, brand_group_sum)
                if row['CP_EC_share %'] > row['R1_EC_share %']:
                    adj_ec_share = row['R1_EC_share %'] + 0.013
                    new_cp_ec = brand_group_sum * adj_ec_share
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                else:
                    adj_ec_share = row['R1_EC_share %'] - 0.013
                    new_cp_ec = brand_group_sum * adj_ec_share
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                self.hit_data.at[index, 'Adj_CP_EC_share %'] = adj_ec_share
                self.hit_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.hit_data.at[index,'Adj_value'] = adj_value

        for index,row in self.hit_data.iterrows():
            # 2、处理'EC month on month exceeds 30%, please check'的情形
            if row['Result'] == condition_5:
                if row['NENC_M2M %'] > 0:
                    adj_cp_ec_m2m = 0.22
                    new_cp_ec = 1.22 * row['Sales Units R1 (E,C)']
                    new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] -1
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                elif row['NENC_M2M %'] == 0:
                    if row['EC_M2M %'] > 0:
                        adj_cp_ec_m2m = 0.22
                        new_cp_ec = 1.22 * row['Sales Units R1 (E,C)']
                        new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                        adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                    else:
                        adj_cp_ec_m2m = -0.22
                        new_cp_ec = 0.78 * row['Sales Units R1 (E,C)']
                        new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                        adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                else:
                    adj_cp_ec_m2m = -0.22
                    new_cp_ec = 0.78 * row['Sales Units R1 (E,C)']
                    new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                self.hit_data.at[index,'Adj_CP_EC_M2M %'] = adj_cp_ec_m2m
                self.hit_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.hit_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m_ration
                self.hit_data.at[index, 'Adj_value'] = adj_value

        for index,row in self.hit_data.iterrows():
            # 3、覆盖
            if pd.isna(row['New_CP_EC']):
                new_cp_ec = row['Sales Units CP (E,C)']
                self.hit_data.at[index,'New_CP_EC'] = new_cp_ec

        for index,row in self.hit_data.iterrows():
            # 4、最后计算因容量动态变化需更新'New_CP_EC_share %','New_CP_EC_share_diff %'两个值
            if row['Result'] == condition_4 or row['Result'] == condition_5:
                brand_value = row['BRAND']
                brand_group_sum = self.hit_data[self.hit_data['BRAND'] == brand_value]['New_CP_EC'].sum()
                new_cp_ec_share = row['New_CP_EC'] / brand_group_sum if brand_group_sum != 0 else 0
                nwe_cp_ec_share_diff_ratio = new_cp_ec_share - row['R1_EC_share %']
                self.hit_data.at[index,'New_CP_EC_share %'] = new_cp_ec_share
                self.hit_data.at[index,'New_CP_EC_share_diff %'] = nwe_cp_ec_share_diff_ratio
        self.hit_data.to_excel(r'Result_hit/hit_result_manual.xlsx', index=False)
        print(orange_text +f'5. The hit_result_manual has been saved in: '
                          f'{os.path.abspath("Result_hit/hit_result_manual.xlsx")}'+ reset_text)
        # 加入人工干涉达到动态调整
        input(red_text + 'Please check and change the hit_result_manual file by manual,press "Enter" to continue working.'+ reset_text)
        hit_data_confirm = pd.read_excel(r'Result_hit/hit_result_manual.xlsx')
        #配平
        def peiping(confirm_file):
            # 初始化新列
            confirm_file['Lable'] = None
            confirm_file['Lable'] = confirm_file['Lable'].astype(object)
            confirm_file['Add'] = None
            confirm_file['Dec'] = None
            confirm_file['Adj_value'] = confirm_file['Adj_value'].fillna(0)
            # 第一部分：计算Lable
            for index, row in confirm_file.iterrows():
                brand_value = row['BRAND']
                lable_sum = confirm_file[confirm_file['BRAND'] == brand_value]['Adj_value'].sum()
                if lable_sum > 0:
                    confirm_file.at[index, 'Lable'] = 'dec'
                elif lable_sum < 0:
                    confirm_file.at[index, 'Lable'] = 'add'
            # 第二部分：计算Add和Dec
            for index, row in confirm_file.iterrows():
                brand_value = row['BRAND']
                brand_sum = confirm_file[confirm_file['BRAND'] == brand_value]['Sales Units CP (E,C)'].sum()
                if (row['Result'] == 'No action required') & (row['Lable'] == 'add') & (
                        row['New_CP_EC'] > 100) & (row['share_diff'] < 0.015) & (row['Sales Units CP (NE,NC)'] > 50):
                    X = (row['R1_EC_share %'] + 0.015) * brand_sum
                    add = X - row['New_CP_EC']
                    confirm_file.at[index, 'Add'] = add
                elif (row['Result'] == 'No action required') & (row['Lable'] == 'dec') & (
                        row['New_CP_EC'] > 100) & (row['share_diff'] > -0.015) & (row['Sales Units CP (NE,NC)'] > 50):
                    X = (row['R1_EC_share %'] - 0.015) * brand_sum
                    dec = X - row['New_CP_EC']
                    if abs(dec) < row['New_CP_EC']:
                        confirm_file.at[index, 'Dec'] = dec
            # 第三部分：分配零合过程
            grouped = confirm_file.groupby('BRAND')
            for brand, group in grouped:
                # 排序
                group = group.sort_values(by='New_CP_EC', ascending=False)
                lable_sum = group['Adj_value'].sum()

                if lable_sum < 0:
                    adj_sum = group['Add'].sum()
                    if abs(lable_sum) > adj_sum:
                        print(f"{brand} can't be statised,please check by manual.")
                        print(f"Label sum for hit {brand}: {lable_sum}")
                        print(f"Add sum for hit {brand}: {adj_sum}\n")
                        continue
                    # lable为'add'的情况
                    add_values = group[group['Add'].notna()]
                    for idx, row in add_values.iterrows():
                        if lable_sum == 0:
                            break
                        adj = min(row['Add'], -lable_sum)
                        confirm_file.at[idx, 'Adj_value'] += adj
                        lable_sum += adj

                elif lable_sum > 0:
                    adj_sum = group['Dec'].sum()
                    if lable_sum > abs(adj_sum):
                        print(f"{brand} can't be statised,please check by manual.")
                        print(f"Label sum for hit {brand}: {lable_sum}")
                        print(f"Add sum for hit {brand}: {adj_sum}\n")
                        continue
                    # lable为'dec'的情况
                    dec_values = group[group['Dec'].notna()]
                    for idx, row in dec_values.iterrows():
                        if lable_sum == 0:
                            break
                        adj = min(abs(row['Dec']), lable_sum)
                        confirm_file.at[idx, 'Adj_value'] -= adj
                        lable_sum -= adj
            # 二次更细'New_CP_EC'
            mask = (confirm_file['Result'] == 'No action required') & (abs(confirm_file['Adj_value']) > 1)
            confirm_file.loc[mask, 'New_CP_EC'] = (
                    confirm_file.loc[mask, 'Sales Units CP (E,C)'] + confirm_file.loc[mask, 'Adj_value']).astype('float64')
            return confirm_file
        self.confirm_file = peiping(hit_data_confirm)
        # 输出格式调整
        unique_brands = self.confirm_file['BRAND'].unique()
        output_excel_path = os.path.join(self.current_directory, 'Result_hit', 'divide_by_key_brand.xlsx')
        with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
            for brand in unique_brands:
                # 提取特定品牌的数据
                brand_data = self.confirm_file[self.confirm_file['BRAND'] == brand].copy()
                # 将城市数据按 ACT 字段升序排列
                brand_data.sort_values(by='New_CP_EC', ascending=False, inplace=True)
                # 将城市数据写入 Excel 中的不同 sheet
                brand_data.to_excel(writer, sheet_name=brand,index=False)
        print(orange_text +f'6. The hit_result_file has been saved in: '
                          f'{os.path.abspath("Result_hit/hit_result.xlsx")}'+ reset_text)
        print('Hit_data logic processing completed\n')
        self.confirm_file.to_excel(r'Result_hit/Total_result.xlsx',index=False)

    def hit_data_bulk(self):
        print('Starting to generate hit_data bulk file, please wait')
        orange_text = '\033[93m'
        reset_text = '\033[0m'
        bulk_range = ['BRAND','Item','Sales Units CP (E,C)','New_CP_EC','Adj_value']
        bulk_select_data = self.confirm_file[bulk_range].copy()
        bulk_select_data = bulk_select_data[abs(bulk_select_data['Adj_value']) > 1]
        bulk_select_data['Hit_key_id'] = bulk_select_data['BRAND'] + '-' + bulk_select_data['Item']
        #重命名，为了后边做映射区分字段用，逻辑上衔接不上，属于编写过程中思考和调试后加的代码
        bulk_select_data.rename(columns={'Sales Units CP (E,C)':'Ori_Sales Units CP (E,C)'},inplace=True)
        new_order = ['BRAND','Item','Hit_key_id','Ori_Sales Units CP (E,C)','New_CP_EC','Adj_value']
        bulk_select_data = bulk_select_data[new_order]
        bulk_select_data.to_excel(r'Result_hit/bulk_range_data.xlsx', index=False)
        print(orange_text +f'7. hit_data bulk file modification scope has been saved in: '
                          f'{os.path.abspath("Result_hit/bulk_range_data.xlsx")}'+ reset_text)

        self.hit_data_bulk_ori['Hit_key_id'] = self.hit_data_bulk_ori['BRAND'] + '-' + self.hit_data_bulk_ori['Item']
        self.hit_data_bulk_ori['Sta_adj'] = None
        for index,row in self.hit_data_bulk_ori.iterrows():
            # 计算满足可调整量
            if row['table'] == 'Y':
                key_value = row['Hit_key_id']
                brand_group_sum = self.hit_data_bulk_ori[(self.hit_data_bulk_ori['Hit_key_id'] == key_value) &
                                                         (self.hit_data_bulk_ori['table'] == 'Y')]['Sales Units CP (E,C)'].sum()
                self.hit_data_bulk_ori.at[index,'Sta_adj'] = brand_group_sum
        # 映射,原理很简单相当于V_lookup函数的功能，代码逻辑较难理解
        fields_to_map = ['Ori_Sales Units CP (E,C)', 'New_CP_EC', 'Adj_value']
        map_data = bulk_select_data[['Hit_key_id'] + fields_to_map] #映射表
        for index, row in self.hit_data_bulk_ori.iterrows():
            if row['table'] == 'Y':
                key_value = row['Hit_key_id']
                matching_row = map_data[map_data['Hit_key_id'] == key_value]
                if not matching_row.empty: #若matching_row不为空，表示有匹配的数据被找到
                    for field in fields_to_map: #接着在fields_to_map列表中遍历需要map的字段
                        self.hit_data_bulk_ori.at[index, field] = matching_row[field].values[0]

        #计算最终bulk结果，并加入代码容错判断，以提高容错率
        printed_ids = set()  # 用于跟踪已经打印过警告消息的 Run_key_id,利用的原理是集合的唯一性，只会唯一保存一个Run_key_id
        self.hit_data_bulk_ori['Mid_value'] = None
        self.hit_data_bulk_ori['K'] = None
        self.hit_data_bulk_ori['Bulk_value'] = None
        for index, row in self.hit_data_bulk_ori.iterrows():
            #该处判断较多，逻辑为：先检测非空条目->判断负值是否够减 最后再逐层展开
            if pd.notna(row['Adj_value']) and pd.notna(row['Sta_adj']):
                if row['Adj_value'] < 0:
                    if row['Sta_adj'] < abs(row['Adj_value']):
                        hit_key_id = row['Hit_key_id']
                        if hit_key_id not in printed_ids:
                            print(f"{hit_key_id} can't be satisfied, please check!")
                            printed_ids.add(hit_key_id)
                    else:
                        mid_value = row['Sta_adj'] + row['Adj_value']
                        k = mid_value / row['Sta_adj']
                        bulk_value = row['Sales Units CP (E,C)'] * k
                        self.hit_data_bulk_ori.at[index,'Mid_value'] = mid_value
                        self.hit_data_bulk_ori.at[index,'K'] = k
                        self.hit_data_bulk_ori.at[index,'Bulk_value'] = bulk_value
                else:
                    mid_value = row['Sta_adj'] + row['Adj_value']
                    k = mid_value / row['Sta_adj']
                    bulk_value = row['Sales Units CP (E,C)'] * k
                    self.hit_data_bulk_ori.at[index, 'Mid_value'] = mid_value
                    self.hit_data_bulk_ori.at[index, 'K'] = k
                    self.hit_data_bulk_ori.at[index, 'Bulk_value'] = bulk_value

        if not printed_ids:
            print("Congratulations! No data that cannot be operated was found in actionable detection.")

        self.hit_data_bulk_ori.to_excel(r'Result_hit/bulk_process.xlsx', index=False)
        print(orange_text + f'8. The bulk_process_file has been saved in: '
                           f'{os.path.abspath("Result_hit/bulk_process.xlsx")}' + reset_text)
        temp = self.hit_data_bulk_ori.copy()
        #还原
        mask = (pd.notna(self.hit_data_bulk_ori['Bulk_value'])) & (self.hit_data_bulk_ori['Bulk_value'] != 0)
        # 确保 `Bulk_value` 列中的数据类型与 `Sales Units CP (E,C)` 列兼容
        self.hit_data_bulk_ori.loc[mask, 'Sales Units CP (E,C)'] = self.hit_data_bulk_ori.loc[
            mask, 'Bulk_value'].astype('float64')
        self.hit_data_bulk_ori.drop(columns=[
            'Sta_adj','Ori_Sales Units CP (E,C)','New_CP_EC','Adj_value','Mid_value',
            'K','Bulk_value'], inplace=True)
        self.hit_data_bulk_ori.to_excel(r'Group_item_from_hit_data_bulk_ori.xlsx',index=False)
        temp_bulk = temp[(pd.notna(temp['Bulk_value'])) & (temp['Bulk_value'] != 0)].copy()
        temp_bulk.loc[:, 'QC ID'] = 257951
        temp_bulk.loc[:, 'CITY2 ID'] = temp_bulk['CITY2 ID'].fillna(temp_bulk['CITY ID'])
        def virtual_bulk_input(temp_bulk):
            select_bulk = ['QC ID', 'Productgroup', 'CITY2', 'CITY2 ID', 'CountryChannel', 'CountryChannel ID',
                           'Outlet ID', 'ORGANISAT TYPE','ORGANISAT TYPE ID', 'COPIES', 'COPIES ID',
                           'BRAND', 'BRAND ID', 'Item', 'Item ID', 'Sales Units CP (E,C)', 'Bulk_value']
            temp_bulk = temp_bulk[select_bulk]
            temp_bulk= temp_bulk.rename(columns={'Productgroup': 'PG ID','Item':'ITEM','Item ID':'ITEM ID',
                                                                            'Sales Units CP (E,C)': 'Current Sales Units CP (E,C)',
                                                                            'Bulk_value': 'New Sales Units CP (E,C)'},inplace=False)
            temp_bulk.to_excel(r'other/hit_bulk.xlsx', index=False)
            print(orange_text + f'9. The hit_bulk file has been saved in: '
                               f'{os.path.abspath("other/hit_bulk.xlsx")}' + reset_text)
            return temp_bulk
        virtual_bulk_input(temp_bulk)
        print('The hit_process has already completed')

class Product_Group():
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.group_data = pd.read_excel('Group_item_from_hit_data_bulk_ori.xlsx')
        self.group_struct = pd.read_excel(r'D_ProductGroup.xlsx')
    def group_data_init(self):
        purple_text = '\033[95m'
        red_text = '\033[91m'
        reset_text = '\033[0m'
        print('Start refactoring group_data.')
        #主品牌锁定
        key_brand = ['APPLE','HUAWEI','HONOR','LENOVO','OPPO','VIVO','XIAOMI','REDMI','SAMSUNG']
        self.group_data = self.group_data[self.group_data['BRAND'].isin(key_brand)]
        # 拆分广州、深圳
        self.group_data.loc[self.group_data['CITY2'] == 'SHENZHEN', 'REGION2'] = 'SHENZHEN'
        # 映射Product_group
        select = ['Item_ID','Product Group']
        select_data = self.group_struct[select]
        select_map = select_data.set_index('Item_ID')['Product Group'].to_dict()
        self.group_data['Product_group'] = self.group_data['Item ID'].map(select_map)
        self.group_data_ori = self.group_data.copy() # 生成bulk使用
        group_data_temp = self.group_data.copy()
        # 检查 'Product_group' 列中是否还存在空值
        if self.group_data['Product_group'].isna().any():
            print(red_text + "Except NULL,Please check."+ reset_text)
            self.group_data[self.group_data['Product_group'].isna()].to_excel(r'Result_group/null.xlsx',index=False)
            print(
                purple_text + f'1. The unmatched data has been saved in: '
                              f'{os.path.abspath("Result_group/null.xlsx")}' + reset_text)
        else:
            print("Matching completed.\n")
        #分组聚合
        self.group_data = self.group_data.groupby(['BRAND','REGION2', 'Product_group', ], as_index=False).agg({
            'Sales Units R1 (NE,NC)': 'sum',
            'Sales Units CP (NE,NC)': 'sum',
            'Sales Units R1 (E,C)': 'sum',
            'Sales Units CP (E,C)': 'sum'
        })
        # print(self.group_data.columns)
        # 计算 "COPIES" 为 "REGULAR" 的聚合值
        regular_copies = group_data_temp[group_data_temp['COPIES'] == 'REGULAR'].groupby(
            ['BRAND', 'REGION2', 'Product_group'], as_index=False).agg({
            'Sales Units R1 (NE,NC)': 'sum',
            'Sales Units CP (NE,NC)': 'sum'
        })
        # 区分
        regular_copies.rename(columns={
            'Sales Units R1 (NE,NC)': 'Sales Units R1 (NE,NC)_REGULAR',
            'Sales Units CP (NE,NC)': 'Sales Units CP (NE,NC)_REGULAR'
        }, inplace=True)
        # 合并两个聚合结果
        self.group_data = pd.merge(self.group_data, regular_copies, on=['BRAND', 'REGION2', 'Product_group'],how='left')
        self.group_data['Sales Units CP (NE,NC)_REGULAR'] = self.group_data['Sales Units CP (NE,NC)_REGULAR'].fillna(0)
        self.group_data['Sales Units R1 (NE,NC)_REGULAR'] = self.group_data['Sales Units R1 (NE,NC)_REGULAR'].fillna(0)
        self.group_data['Group_key_id'] = self.group_data['REGION2'] + '-' + self.group_data['BRAND']
        #计算Product_group在个省内Brand下的占比
        self.group_data['R1_EC_share %(single_region)'] = self.group_data.groupby('Group_key_id')[
            'Sales Units R1 (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.group_data['CP_EC_share %(single_region)'] = self.group_data.groupby('Group_key_id')[
            'Sales Units CP (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.group_data['share_diff'] = self.group_data['CP_EC_share %(single_region)'] - self.group_data['R1_EC_share %(single_region)']
        # 顺序显示
        new_order = ['BRAND','REGION2','Product_group','Group_key_id','Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)',
                     'Sales Units R1 (E,C)', 'Sales Units CP (E,C)','R1_EC_share %(single_region)','CP_EC_share %(single_region)',
                     'share_diff','Sales Units R1 (NE,NC)_REGULAR','Sales Units CP (NE,NC)_REGULAR']
        self.group_data = self.group_data[new_order]
        # 异常
        conditions = [(abs(self.group_data['share_diff']) >= 0.07) &
                      ((self.group_data['Sales Units R1 (NE,NC)'] > 0) & (self.group_data['Sales Units CP (E,C)'] > 0))]
        values = ['Share difference exceeds 7%, please check']
        self.group_data['Result'] = np.select(conditions, values, default='No action required')
        self.group_data.to_excel(r'Result_group/group_file_0.xlsx',index=False)
        print(purple_text +f'2. The group_file_0 has been saved in: {os.path.abspath("Result_group/group_file_0.xlsx")}'+ reset_text)
        print('Group_data reconstruction completed.\n')


    def group_data_func(self):
        print('Start group_data logical processing of data')
        global condition_6
        purple_text = '\033[95m'
        red_text = '\033[91m'
        reset_text = '\033[0m'
        self.group_data['Adj_CP_EC_share %'] = None #调整至份额占比
        self.group_data['New_CP_EC'] = None #新CP（E,C）值
        self.group_data['New_CP_EC_share %'] = None #新CP（E,C）值份额占比
        self.group_data['New_CP_EC_share_diff %'] = None #新CP（E,C）值份额占比差值
        self.group_data['Adj_value'] = None #最终调整值

        for index, row in self.group_data.iterrows():
            # 1、处理'Share difference exceeds 7%, please check'的情形
            if row['Result'] == condition_6:
                group_value = row['Group_key_id']
                group_sum = self.group_data[self.group_data['Group_key_id'] == group_value]['Sales Units CP (E,C)'].sum()
                if row['CP_EC_share %(single_region)'] > row['R1_EC_share %(single_region)']:
                    adj_ec_share = row['R1_EC_share %(single_region)'] + 0.018
                    new_cp_ec = group_sum * adj_ec_share
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                else:
                    adj_ec_share = row['R1_EC_share %(single_region)'] - 0.018
                    new_cp_ec = group_sum * adj_ec_share
                    adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                self.group_data.at[index, 'Adj_CP_EC_share %'] = adj_ec_share
                self.group_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.group_data.at[index,'Adj_value'] = adj_value

        for index,row in self.group_data.iterrows():
            # 3、覆盖
            if pd.isna(row['New_CP_EC']):
                new_cp_ec = row['Sales Units CP (E,C)']
                self.group_data.at[index,'New_CP_EC'] = new_cp_ec

        for index,row in self.group_data.iterrows():
            # 2、计算因容量动态变化需更新'New_CP_EC_share %','New_CP_EC_share_diff %'两个值
            if row['Result'] == condition_6:
                group_value = row['Group_key_id']
                new_group_sum = self.group_data[self.group_data['Group_key_id'] == group_value]['New_CP_EC'].sum()
                new_cp_ec_share = row['New_CP_EC'] / new_group_sum if new_group_sum != 0 else 0
                nwe_cp_ec_share_diff_ratio = new_cp_ec_share - row['R1_EC_share %(single_region)']
                self.group_data.at[index,'New_CP_EC_share %'] = new_cp_ec_share
                self.group_data.at[index,'New_CP_EC_share_diff %'] = nwe_cp_ec_share_diff_ratio

        self.group_data.to_excel(r'Result_group/group_result_manual.xlsx', index=False)
        print(purple_text +f'3. The group_result_manual has been saved in: '
                          f'{os.path.abspath("Result_group/group_result_manual.xlsx")}'+ reset_text)
        # 人工接口
        input(red_text + 'Please check and change the group_result_manual file by manual,press "Enter" to continue working.\n'+ reset_text)
        group_data_confirm = pd.read_excel(r'Result_group/group_result_manual.xlsx')

        #配平
        def peiping(confirm_file):
            # 控制有效数据提高运行速度
            filtered_df = confirm_file[abs(confirm_file['Adj_value']) > 1]
            unique_group_keys = filtered_df['Group_key_id'].unique()
            new_confirm_file = confirm_file[confirm_file['Group_key_id'].isin(unique_group_keys)].copy()

            new_confirm_file.loc[:, 'Lable'] = None
            new_confirm_file['Lable'] = new_confirm_file['Lable'].astype(object)
            new_confirm_file.loc[:, 'Add'] = None
            new_confirm_file.loc[:, 'Dec'] = None
            new_confirm_file['Adj_value'] = new_confirm_file['Adj_value'].fillna(0)
            # 第一部分：标记Label
            for index, row in new_confirm_file.iterrows():
                group_value = row['Group_key_id']
                lable_sum = new_confirm_file[new_confirm_file['Group_key_id'] == group_value]['Adj_value'].sum()
                if lable_sum > 0:
                    new_confirm_file.at[index, 'Lable'] = 'dec'
                elif lable_sum < 0:
                    new_confirm_file.at[index, 'Lable'] = 'add'
            # 第二部分：计算Add和Dec
            for index, row in new_confirm_file.iterrows():
                group_value = row['Group_key_id']
                lable_sum = new_confirm_file[new_confirm_file['Group_key_id'] == group_value]['Sales Units CP (E,C)'].sum()

                if (row['Result'] == 'No action required') and (row['Lable'] == 'add') and (
                        row['New_CP_EC'] > 0) and (row['share_diff'] < 0.035) and (row['Sales Units CP (NE,NC)'] > 0):
                    X = (row['R1_EC_share %(single_region)'] + 0.035) * lable_sum
                    add = X - row['New_CP_EC']
                    new_confirm_file.at[index, 'Add'] = add
                elif ((row['Result'] == 'No action required') and (row['Lable'] == 'dec') and (
                        row['New_CP_EC'] > 0) and (row['share_diff'] > -0.035) and (row['Sales Units CP (NE,NC)'] > 0) and
                      (row['R1_EC_share %(single_region)'] > 0)) :
                    X = (row['R1_EC_share %(single_region)'] - 0.035) * lable_sum
                    dec = X - row['New_CP_EC']
                    if abs(dec) < row['New_CP_EC']:
                        new_confirm_file.at[index, 'Dec'] = dec
                # 新品处理
                elif ((row['Result'] == 'No action required') and (row['Lable'] == 'dec') and (
                        row['New_CP_EC'] > 0) and (row['share_diff'] > -0.035) and (row['Sales Units CP (NE,NC)'] > 0) and
                      (row['R1_EC_share %(single_region)'] == 0)):
                    X = 0.033 * lable_sum
                    dec = X - row['New_CP_EC']
                    if abs(dec) < row['New_CP_EC']:
                        new_confirm_file.at[index, 'Dec'] = dec
            new_confirm_file.to_excel(r'Result_group/计算后.xlsx')
            # 第三部分：分配零合过程
            grouped = new_confirm_file.groupby('Group_key_id')
            for brand, group in grouped:
                # print(f"Processing group {brand}")  # 调试信息
                group = group.sort_values(by='New_CP_EC', ascending=False)
                lable_sum = group['Adj_value'].sum()
                # print(f"Label sum for group {brand}: {lable_sum}")  # 调试信息

                if lable_sum < 0:
                    adj_sum = group['Add'].sum()
                    # print(f"Add sum for group {brand}: {adj_sum}")  # 调试信息
                    if abs(lable_sum) > adj_sum:
                        print(f"{brand} can't be satisfied, please check manually.")
                        print(f"Label sum for group {brand}: {lable_sum}")
                        print(f"Add sum for group {brand}: {adj_sum}\n")
                        continue  # 跳过当前分组，继续处理下一个分组
                    # lable为'add'的情况
                    add_values = group[group['Add'].notna()]
                    for idx, row in add_values.iterrows():
                        if lable_sum == 0:
                            break
                        adj = min(row['Add'], -lable_sum)
                        new_confirm_file.at[idx, 'Adj_value'] += adj
                        lable_sum += adj
                elif lable_sum > 0:
                    adj_sum = group['Dec'].sum()
                    # print(f"Dec sum for group {brand}: {adj_sum}")  # 调试信息
                    if lable_sum > abs(adj_sum):
                        print(f"{brand} can't be satisfied, please check manually.")
                        print(f"Label sum for group {brand}: {lable_sum}")
                        print(f"Dec sum for group {brand}: {adj_sum}\n")
                        continue  # 跳过当前分组，继续处理下一个分组
                    # lable为'dec'的情况
                    dec_values = group[group['Dec'].notna()]
                    for idx, row in dec_values.iterrows():
                        if lable_sum == 0:
                            break
                        adj = min(abs(row['Dec']), lable_sum)
                        new_confirm_file.at[idx, 'Adj_value'] -= adj
                        lable_sum -= adj
            # 二次更细'New_CP_EC'
            mask = (new_confirm_file['Result'] == 'No action required') & (abs(new_confirm_file['Adj_value']) > 1)
            new_confirm_file.loc[mask, 'New_CP_EC'] = (
                    new_confirm_file.loc[mask, 'Sales Units CP (E,C)'] + new_confirm_file.loc[mask, 'Adj_value']).astype('float64')
            new_confirm_file.to_excel(r'Result_group/group_result_manual_2.xlsx')
            return new_confirm_file
        self.confirm_file = peiping(group_data_confirm)
        self.confirm_file.to_excel(r'Result_group/group_result_manual_2.xlsx')  # 可能会存在算法无法实现配平的结构组
        # 再一次人工检查
        input(red_text + 'Please check and change the group_result_manual_2 file by manual,press "Enter" to continue working.\n'+ reset_text)
        self.confirm_file = pd.read_excel(r'Result_group/group_result_manual_2.xlsx')
        '''
        按照结构组拆分的格式，需要可以打开
        unique_group = self.confirm_file['Group_key_id'].unique()
        output_excel_path = os.path.join(self.current_directory, 'Result_group', 'divide_by_group_key_id.xlsx')
        with pd.ExcelWriter(output_excel_path, engine='xlsxwriter') as writer:
            for group in unique_group:
                group_data = self.confirm_file[self.confirm_file['Group_key_id'] == group].copy()
                # 将城市数据按 ACT 字段升序排列
                group_data.sort_values(by='New_CP_EC', ascending=False, inplace=True)
                # 将城市数据写入 Excel 中的不同 sheet
                group_data.to_excel(writer, sheet_name=group,index=False)
        self.confirm_file.to_excel(r'Result_group/Total_result.xlsx',index=False)
        '''

    def group_data_bulk(self):
        # start_time = time.time()
        print('Starting to generate group_data bulk file, please wait')
        purple_text = '\033[95m'
        reset_text = '\033[0m'
        bulk_range = ['Group_key_id','Product_group','Sales Units CP (E,C)','New_CP_EC','Adj_value']
        bulk_select_data = self.confirm_file[bulk_range].copy()
        bulk_select_data = bulk_select_data[abs(bulk_select_data['Adj_value']) > 1]
        bulk_select_data['Group_bulk_key_id'] = bulk_select_data['Group_key_id'] + '-' + bulk_select_data['Product_group']
        #重命名，为了后边做映射区分字段用，逻辑上衔接不上，属于编写过程中思考和调试后加的代码
        bulk_select_data.rename(columns={'Sales Units CP (E,C)':'Ori_Sales Units CP (E,C)'},inplace=True)
        new_order = ['Group_key_id','Product_group','Group_bulk_key_id','Ori_Sales Units CP (E,C)','New_CP_EC','Adj_value']
        bulk_select_data = bulk_select_data[new_order]
        bulk_select_data.to_excel(r'Result_hit/bulk_range_data.xlsx', index=False)
        print(purple_text +f'4. group_data bulk file modification scope has been saved in: '
                          f'{os.path.abspath("Result_hit/bulk_range_data.xlsx")}'+ reset_text)

        self.group_data_ori['Group_key_id'] = (self.group_data_ori['REGION2'] + '-' +
                                               self.group_data_ori['BRAND'] + '-' + self.group_data_ori['Product_group'])
        self.group_data_ori['Sta_adj'] = None
        for index,row in self.group_data_ori.iterrows():
            # 计算满足可调整量
            if row['table'] == 'Y':
                key_value = row['Group_key_id']
                group_sum = self.group_data_ori[(self.group_data_ori['Group_key_id'] == key_value) &
                                                         (self.group_data_ori['table'] == 'Y')]['Sales Units CP (E,C)'].sum()
                self.group_data_ori.at[index,'Sta_adj'] = group_sum
        # 映射,原理很简单相当于V_lookup函数的功能，代码逻辑较难理解
        fields_to_map = ['Ori_Sales Units CP (E,C)', 'New_CP_EC', 'Adj_value']
        map_data = bulk_select_data[['Group_bulk_key_id'] + fields_to_map] #映射表
        for index, row in self.group_data_ori.iterrows():
            if row['table'] == 'Y':
                key_value = row['Group_key_id']
                matching_row = map_data[map_data['Group_bulk_key_id'] == key_value]
                if not matching_row.empty: #若matching_row不为空，表示有匹配的数据被找到
                    for field in fields_to_map: #接着在fields_to_map列表中遍历需要map的字段
                        self.group_data_ori.at[index, field] = matching_row[field].values[0]

        #计算最终bulk结果，并加入代码容错判断，以提高容错率
        printed_ids = set()  # 用于跟踪已经打印过警告消息的 Run_key_id,利用的原理是集合的唯一性，只会唯一保存一个Run_key_id
        self.group_data_ori['Mid_value'] = None
        self.group_data_ori['K'] = None
        self.group_data_ori['Bulk_value'] = None
        for index, row in self.group_data_ori.iterrows():
            #该处判断较多，逻辑为：先检测非空条目->判断负值是否够减 最后再逐层展开
            if pd.notna(row['Adj_value']) and pd.notna(row['Sta_adj']):
                if row['Adj_value'] < 0:
                    if row['Sta_adj'] < abs(row['Adj_value']):
                        group_key_id = row['Group_key_id']
                        if group_key_id not in printed_ids:
                            print(f"{group_key_id} can't be satisfied, please check!")
                            printed_ids.add(group_key_id)
                            # print(printed_ids) #测试
                    else:
                        mid_value = row['Sta_adj'] + row['Adj_value']
                        k = mid_value / row['Sta_adj'] if row['Sta_adj'] != 0 else 0
                        bulk_value = row['Sales Units CP (E,C)'] * k
                        self.group_data_ori.at[index,'Mid_value'] = mid_value
                        self.group_data_ori.at[index,'K'] = k
                        self.group_data_ori.at[index,'Bulk_value'] = bulk_value
                else:
                    mid_value = row['Sta_adj'] + row['Adj_value']
                    k = mid_value / row['Sta_adj'] if row['Sta_adj'] != 0 else 0
                    bulk_value = row['Sales Units CP (E,C)'] * k
                    self.group_data_ori.at[index, 'Mid_value'] = mid_value
                    self.group_data_ori.at[index, 'K'] = k
                    self.group_data_ori.at[index, 'Bulk_value'] = bulk_value

        if printed_ids is None:
            print("Congratulations! No data that cannot be operated was found in actionable detection.")

        self.group_data_ori.to_excel(r'Result_group/bulk_process.xlsx', index=False)
        print(purple_text + f'5. The bulk_process_file has been saved in: '
                           f'{os.path.abspath("Result_group/bulk_process.xlsx")}' + reset_text)
        temp = self.group_data_ori.copy()
        #还原
        mask = (pd.notna(self.group_data_ori['Bulk_value'])) & (self.group_data_ori['Bulk_value'] != 0)
        # 确保 `Bulk_value` 列中的数据类型与 `Sales Units CP (E,C)` 列兼容
        self.group_data_ori.loc[mask, 'Sales Units CP (E,C)'] = self.group_data_ori.loc[
            mask, 'Bulk_value'].astype('float64')
        self.group_data_ori.drop(columns=[
            'Sta_adj','Ori_Sales Units CP (E,C)','New_CP_EC','Adj_value','Mid_value',
            'K','Bulk_value'], inplace=True)
        self.group_data_ori.to_excel(r'FINAL_DATA.xlsx',index=False)

        temp_bulk = temp[(pd.notna(temp['Bulk_value'])) & (temp['Bulk_value'] != 0)].copy() # 去重，用作最终bulk file
        group_bulk = temp[(pd.notna(temp['Bulk_value'])) & (temp['Bulk_value'] != 0)].copy() # 聚合新值
        temp_bulk.loc[:, 'QC ID'] = 257951
        temp_bulk.loc[:, 'CITY2 ID'] = temp_bulk['CITY2 ID'].fillna(temp_bulk['CITY ID'])

        def virtual_bulk_input(temp_bulk,group_bulk):
            select_bulk = ['QC ID', 'Productgroup', 'CITY2', 'CITY2 ID', 'CountryChannel', 'CountryChannel ID',
                           'Outlet ID', 'ORGANISAT TYPE','ORGANISAT TYPE ID', 'COPIES', 'COPIES ID',
                           'BRAND', 'BRAND ID', 'Item', 'Item ID']
            temp_bulk = temp_bulk[select_bulk]
            temp_bulk= temp_bulk.rename(columns={'Productgroup': 'PG ID','Item':'ITEM','Item ID':'ITEM ID'},inplace=False)
            temp_bulk = temp_bulk.drop_duplicates()
            temp_bulk['Outlet ID'] = temp_bulk['Outlet ID'].astype(str)
            temp_bulk['ITEM ID'] = temp_bulk['ITEM ID'].astype(str)
            temp_bulk['id'] = temp_bulk['Outlet ID'] + '-' + temp_bulk['ITEM ID']
            temp_bulk.to_excel(r'Result_group/test_1.xlsx')
            group_bulk['Outlet ID'] = group_bulk['Outlet ID'].astype(str)
            group_bulk['Item ID'] = group_bulk['Item ID'].astype(str)
            group_bulk['id'] = group_bulk['Outlet ID'] + group_bulk['Item ID']
            group_bulk = group_bulk.groupby(['id'], as_index=False).agg({
                'Sales Units CP (E,C)': 'sum',
                'Bulk_value': 'sum'})
            group_bulk= group_bulk.rename(columns={'Sales Units CP (E,C)': 'Current Sales Units CP (E,C)',
                                                   'Bulk_value':'New Sales Units CP (E,C)'},inplace=False)
            group_bulk.to_excel(r'Result_group/test_2.xlsx')
            temp_bulk = pd.merge(temp_bulk, group_bulk, on=['id'],how='left')

            temp_bulk = temp_bulk.drop(columns=['id'])
            temp_bulk.to_excel(r'other/group_bulk.xlsx', index=False)
            print(purple_text + f'6. The group_bulk file has been saved in: '
                               f'{os.path.abspath("other/group_bulk.xlsx")}' + reset_text)
            return temp_bulk,group_bulk
        virtual_bulk_input(temp_bulk,group_bulk)
        print('The group_process has already completed')



if __name__ == '__main__':
    # print('一、 The 【RUN_data】 Process Start......\n')
    # start_time_0 = time.time()
    # run = RUN_city_by_region()
    # run.run_data_init()
    # condition_0 = 'Share difference exceeds 5%, please check'
    # condition_1 = 'EC month on month exceeds 50%, please check'
    # condition_2 = 'Not trend V1, please check'
    # condition_3 = 'Not trend V2, please check'
    # # 这两个变量因为我想变成全局变量使用，所以需要放在函数之外（可以被所有想访问的资源访问的新内存地址中），全局变量可以节省代码量
    # run.run_data_func()
    # run.run_data_bulk()
    # end_time_0 = time.time()
    # minutes = round((end_time_0 - start_time_0) / 60,2)
    # print(f"The run_data proces execution time: {minutes} minutes\n")

    # print('二、 The 【HIT_data】 Process Start......\n')
    # start_time_1 = time.time()
    # hit = HITLIST_KEY_brand()
    # hit.hit_data_init()
    # condition_4 = 'Share difference exceeds 3%, please check'
    # condition_5 = 'EC month on month exceeds 30%, please check'
    # hit.hit_data_func()
    # hit.hit_data_bulk()
    # end_time_1 = time.time()
    # minutes = round((end_time_1 - start_time_1) / 60,2)
    # print(f"The hit_data proces execution time: {minutes} minutes\n")

    # print('三、 The 【GOUP_data】 Process Start......\n')
    # start_time_2 = time.time()
    # group = Product_Group()
    # group.group_data_init()
    # condition_6 = 'Share difference exceeds 7%, please check'
    # group.group_data_func()
    # group.group_data_bulk()
    # end_time_2 = time.time()
    # minutes = round((end_time_2 - start_time_2) / 60,2)
    # print(f"The hit_data proces execution time: {minutes} minutes\n")

    print('四、The main proces has completed，running the calculate.py...')
    path = r'D:\study\work_code\WEB\other\calculate.py'
    subprocess.run([sys.executable, path])
    print('The calculate.py has completed.')

