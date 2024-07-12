import pandas as pd
import os
import sys
class HITLIST_KEY_brand():
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.confirm_file = pd.read_excel('group_result_manual.xlsx')

    def demo(self):
        filtered_df = self.confirm_file[abs(self.confirm_file['Adj_value']) > 1]
        unique_group_keys = filtered_df['Group_key_id'].unique()
        self.confirm_file = self.confirm_file[self.confirm_file['Group_key_id'].isin(unique_group_keys)]
        print(self.confirm_file)
        # print(len(unique_group_keys))
        # print(unique_group_keys)


if __name__ == '__main__':
    group = HITLIST_KEY_brand()
    group.demo()

'''
# 纵向计算product_group在各省的占比
product_group_data = self.group_data.copy()
product_group_data = product_group_data.groupby(['Product_group'], as_index=False).agg({
    'Sales Units R1 (E,C)': 'sum',
    'New_CP_EC': 'sum'
})
product_group_data.rename(columns={
    'Sales Units R1 (E,C)': 'Sales Units R1 (E,C)_Group_sum',
    'New_CP_EC': 'Sales Units CP (E,C)_Group_sum'
}, inplace=True)
self.group_data = pd.merge(self.group_data, product_group_data, on=['Product_group'],how='left')

# 百思不得其姐----------------------------------------------------------------------------------------------------------
self.group_data['Sales Units CP (E,C)_Group_sum'].fillna(0, inplace=True)
self.group_data['Sales Units R1 (E,C)_Group_sum'].fillna(0, inplace=True)
self.group_data['Sales Units R1 (E,C)_Group_sum'].replace(0, np.nan, inplace=True)
self.group_data['Sales Units CP (E,C)_Group_sum'].replace(0, np.nan, inplace=True)
# 百思不得其姐----------------------------------------------------------------------------------------------------------

self.group_data['R1_EC_share %(total_region)'] = np.where(self.group_data['Sales Units R1 (E,C)_Group_sum'] != 0,
                        self.group_data['Sales Units R1 (E,C)'] / self.group_data['Sales Units R1 (E,C)_Group_sum'], 0)
self.group_data['CP_EC_share %(total_region)'] = np.where(self.group_data['Sales Units CP (E,C)_Group_sum'] != 0,
                        self.group_data['New_CP_EC'] / self.group_data['Sales Units CP (E,C)_Group_sum'], 0)
self.group_data['R1_EC_share %(total_region)'].fillna(0, inplace=True)
self.group_data['CP_EC_share %(total_region)'].fillna(0, inplace=True)
self.group_data.to_excel(r'Result_group/group_region.xlsx', index=False)
'''