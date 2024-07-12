import pandas as pd
import os
import sys

class Hello():
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.demo_1 = pd.read_excel('WEB Offline 100C MONTHLY_2404M.xlsx')
        self.demo_2 = pd.read_excel('bulk_9.xlsx')


    def act(self):
        self.demo['CITY2 ID'].fillna(self.demo['CITY ID'], inplace=True)
        self.demo.to_excel(r'Result_run/bulk_process_0.xlsx')

if __name__ == '__main__':
    hello = Hello() #实例化对象
    hello.act()