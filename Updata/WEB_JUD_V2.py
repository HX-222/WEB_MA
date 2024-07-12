import pandas as pd
import os
import sys
import numpy as np
import time

class RUN_city_by_region():
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.file_path = 'WEB Offline 100C MONTHLY_2404M.xlsx'
        self.run_data = pd.read_excel(self.file_path)  # 计算使用
        # !!!!优化点 0 ！！！！
        # 避免重复读取文件
        self.run_data_bulk_ori = self.run_data.copy()  # 生成bulk文件及模拟bulk进入系统使用

    def run_data_init(self):
        # 在这个函数中，我打算用来执行处理数据文件的功能，也就是说将原始数据预处理成为我想要的文件样式，在进行后续编写和处理，分层编写，逻辑更清晰
        print('Start refactoring run_data.')
        start_time = time.time()
        green_text = '\033[92m'
        reset_text = '\033[0m'

        # 筛选稳定可比店
        self.run_data = self.run_data.fillna(0)
        run_data_0 = self.run_data[self.run_data['COPIES'] == 'REGULAR']
        run_data_0 = run_data_0.groupby('Outlet').agg({
            'Sales Units R1 (NE,NC)': 'sum',
            'Sales Units CP (NE,NC)': 'sum'
        })
        run_data_0 = run_data_0[(run_data_0['Sales Units R1 (NE,NC)'] != 0) & (run_data_0['Sales Units CP (NE,NC)'] != 0)]
        run_data_0.reset_index(inplace=True)
        run_data_0.to_excel(r'Result_run/useful_outlet.xlsx', index=False)
        not_in_run_data_0 = ~self.run_data['Outlet'].isin(run_data_0['Outlet'])
        self.run_data.loc[not_in_run_data_0, ['Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)']] = 0
        self.run_data.to_excel(r'Result_run/ori.xlsx', index=False)

        # 分组聚合
        self.run_data = self.run_data.groupby(['REGION2', 'CITY2', 'BRAND'], as_index=False).agg({
            'Sales Units R1 (NE,NC)': 'sum',
            'Sales Units CP (NE,NC)': 'sum',
            'Sales Units R1 (E,C)': 'sum',
            'Sales Units CP (E,C)': 'sum'
        })
        # ！！！！优化点 1 ！！！！
         #groupby 结合 agg 进行总和计算，相比直接使用 pivot_table 进行总和计算更有优势，详细可以自己查看
        self.run_data['CITY3'] = self.run_data['REGION2'] + '-' + self.run_data['CITY2']
        new_order = ['REGION2', 'CITY2', 'CITY3', 'BRAND', 'Sales Units R1 (NE,NC)', 'Sales Units CP (NE,NC)',
                     'Sales Units R1 (E,C)', 'Sales Units CP (E,C)']
        self.run_data = self.run_data[new_order]
        self.run_data.to_excel(r'Result_run/run_file_1.xlsx', index=False)
        print(green_text + f'1. The run_file_1 has been saved in: {os.path.abspath("Result_run/run_file_1.xlsx")}' + reset_text)

        # 计算各关键值
        self.run_data['R1_NENC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units R1 (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['CP_NENC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units CP (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['R1_EC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units R1 (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['CP_EC_share %'] = self.run_data.groupby('CITY3')[
            'Sales Units CP (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.run_data['share_diff'] = self.run_data['CP_EC_share %'] - self.run_data['R1_EC_share %']

        # ！！！！优化点 2 ！！！！
        # 向量化操作
        '''
        apply 方法： apply 方法可以用于 DataFrame 或 Series 对象，用于沿着指定的轴（行或列）应用函数
        通常情况下，apply 会逐行（或逐列）迭代执行函数，这在处理每行数据需要特定逻辑的情况下非常有用
        向量化操作： 在数据处理中，向量化操作是指对整个数据结构（如 pandas 的 Series 或 DataFrame）执行的一次性操作，而不是逐个元素进行循环操作
         np.where 是 NumPy 提供的函数，用于根据条件返回数组中的值。它能够基于条件一次性对整个数组执行操作，避免了显式的循环，从而提升了计算效率
        '''
        self.run_data['NENC_M2M %'] = np.where(self.run_data['Sales Units R1 (NE,NC)'] != 0,
                                                (self.run_data['Sales Units CP (NE,NC)'] / self.run_data['Sales Units R1 (NE,NC)'] - 1), 0)
        self.run_data['EC_M2M %'] = np.where(self.run_data['Sales Units R1 (E,C)'] != 0,
                                             (self.run_data['Sales Units CP (E,C)'] / self.run_data['Sales Units R1 (E,C)'] - 1), 0)
        self.run_data.to_excel(r'Result_run/run_file_2.xlsx', index=False)
        print(green_text + f'2. The run_file_2 has been saved in: {os.path.abspath("Result_run/run_file_2.xlsx")}' + reset_text)

        # 逻辑判断值编写
        conditions = [
            abs(self.run_data['share_diff']) >= 0.05,
            (abs(self.run_data['EC_M2M %']) >= 0.5) & (self.run_data['Sales Units CP (E,C)'] > 50),

            (self.run_data['Sales Units CP (E,C)'] != 0) & (self.run_data['NENC_M2M %'] * self.run_data['EC_M2M %'] < 0) &
            ((self.run_data['Sales Units R1 (NE,NC)'] > 50) & (self.run_data['Sales Units CP (NE,NC)'] > 50)),

            (self.run_data['Sales Units CP (E,C)'] != 0) & (self.run_data['NENC_M2M %'] * self.run_data['EC_M2M %'] < 0) &
            ((self.run_data['Sales Units R1 (NE,NC)'] <= 50) & (self.run_data['Sales Units CP (NE,NC)'] <= 50))
        ]
        values = [
            'Share difference exceeds 5%, please check',
            'EC month on month exceeds 50%, please check',
            'Not trend V1, please check',
            'Not trend V2, please check'
        ]
        self.run_data['Result'] = np.select(conditions, values, default='No action required')

        self.run_data.to_excel(r'Result_run/run_file_3.xlsx', index=False)
        print(green_text + f'3. The run_file_3 has been saved in: {os.path.abspath("Result_run/run_file_3.xlsx")}' + reset_text)
        print('Run_data reconstruction completed.\n')
        end_time = time.time()
        print(f"Execution time: {end_time - start_time} seconds")

    def run_data_func(self):
        print('Start run_data logical processing of data')
        start_time = time.time()
        conditions = {
            'Share difference exceeds 5%, please check': 'condition_0',
            'EC month on month exceeds 50%, please check': 'condition_1',
            'Not trend V1, please check': 'condition_2', # 样本比较好的情形
            'Not trend V2, please check': 'condition_3'
        }

        for index, row in self.run_data.iterrows():
            result = row['Result']
            if result == conditions['Share difference exceeds 5%, please check']:
                city_value = row['CITY3']
                city_group_sum = self.run_data[self.run_data['CITY3'] == city_value]['Sales Units CP (E,C)'].sum()

                if row['CP_EC_share %'] > row['R1_EC_share %']:
                    adj_ec_share = row['R1_EC_share %'] + 0.015
                else:
                    adj_ec_share = row['R1_EC_share %'] - 0.015
                new_cp_ec = city_group_sum * adj_ec_share
                adj_value = new_cp_ec - row['Sales Units CP (E,C)']
                new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] - 1

                self.run_data.at[index, 'Adj_CP_EC_share %'] = adj_ec_share
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index, 'Adj_value'] = adj_value
                self.run_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m

            elif result == conditions['EC month on month exceeds 50%, please check']:
                if row['NENC_M2M %'] > 0:
                    adj_cp_ec_m2m = 0.22
                    new_cp_ec = 1.22 * row['Sales Units R1 (E,C)']
                else:
                    adj_cp_ec_m2m = -0.22
                    new_cp_ec = 0.78 * row['Sales Units R1 (E,C)']

                new_cp_ec_m2m_ration = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                adj_value = new_cp_ec - row['Sales Units CP (E,C)']

                self.run_data.at[index, 'Adj_CP_EC_M2M %'] = adj_cp_ec_m2m
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m_ration
                self.run_data.at[index, 'Adj_value'] = adj_value

            elif result == conditions['Not trend V1, please check']:
                adj_cp_ec_m2m = row['NENC_M2M %'] * 0.6
                mid = 1 + adj_cp_ec_m2m
                new_cp_ec = row['Sales Units R1 (E,C)'] * mid
                new_cp_ec_m2m = new_cp_ec / row['Sales Units R1 (E,C)'] - 1
                adj_value = new_cp_ec - row['Sales Units CP (E,C)']

                self.run_data.at[index, 'Adj_CP_EC_M2M %'] = adj_cp_ec_m2m
                self.run_data.at[index, 'New_CP_EC'] = new_cp_ec
                self.run_data.at[index, 'New_CP_EC_M2M %'] = new_cp_ec_m2m
                self.run_data.at[index, 'Adj_value'] = adj_value

            elif result == conditions['Not trend V2, please check']:
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

        self.run_data.to_excel(r'Result_run/111.xlsx')


        # 处理未计算的New_CP_EC字段
        self.run_data['New_CP_EC'].fillna(self.run_data['Sales Units CP (E,C)'], inplace=True)

        # 计算New_CP_EC_share % 和 New_CP_EC_share_diff %
        city_group_sum = self.run_data.groupby('CITY3')['New_CP_EC'].transform('sum')
        self.run_data['New_CP_EC_share %'] = self.run_data['New_CP_EC'] / city_group_sum
        self.run_data['New_CP_EC_share_diff %'] = self.run_data['New_CP_EC_share %'] - self.run_data['R1_EC_share %']

        # 保存结果
        self.run_data.to_excel(r'Result_run/run_result.xlsx', index=False)
        print('4. The run_result_file has been saved in:', os.path.abspath("Result_run/run_result.xlsx"))
        print('Run_data logic processing completed\n')
        end_time = time.time()
        print(f"Execution time: {end_time - start_time} seconds")


if __name__ == '__main__':
    print('Process Start......\n')
    run = RUN_city_by_region()
    run.run_data_init()
    run.run_data_func()
