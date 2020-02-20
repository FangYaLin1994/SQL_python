import pandas as pd
import sqlite3, csv, os
from datetime import datetime
import time

con = sqlite3.connect('LVR.db') #連結database
cor = con.cursor()

#第1步:從manifest.csv擷取資料表名稱
Table_Name_All = [str(x[0]) for x in cor.execute("select name from 'manifest.csv';")]  #資料表 ex:a_lvr_land_a.csv

#第2步:整理出所需資料並建立dic
#select * from 'a_lvr_land_a.csv' left join 'a_lvr_land_a_build.csv' on 'a_lvr_land_a.csv'.編號 = 'a_lvr_land_a_build.csv'.編號 group by 'a_lvr_land_a.csv'.編號
def Count_Age(Buy_Time,Build_Time):#計算屋齡
    if Build_Time is None:
        return '未知'
    else:
        buy_time = str((int(Buy_Time)+19110000)//100)
        build_time = str((int(Build_Time)+19110000)//100)
        time_form = "%Y%m"
        house_age = datetime.strptime(buy_time, time_form) - datetime.strptime(build_time, time_form)
        result = int(house_age.days)
        if result < 3650:
            return '10年以下'
        elif  3650 <= result < 7300:
            return '10年~20年'
        elif  7300 <= result < 10950:
            return '20年~30年'
        else:
            return '30年以上'

Data = {
    '所屬縣市':[],
    '鄉鎮市區':[],
    '交易屋齡':[],
    '主要用途(住家用)':[],
    '主要用途(商業用)':[],
    '主要用途(工業用)':[],
    '主要用途(住商用)':[],
    '主要用途(其他)':[],
    '主要建材(鋼骨)':[],
    '主要建材(磚造)':[],
    '主要建材(其他)':[],
    '總計':[]
}
for i in range(len(Table_Name_All)):
    check = [x[1] for x in cor.execute("PRAGMA table_info('%s');" % Table_Name_All[i])] #確認每個資料表中的欄位名稱
    if '交易筆棟數' in check:
        Raw_Data = [x for x in cor.execute("select * from '%s' where not (鄉鎮市區 = 'The villages and towns urban district');" % Table_Name_All[i])] 
        for y in Raw_Data:#y[29],y[1],y[8],y[15],y[13]用途,y[14]建材
            Data['所屬縣市'].append(y[29])
            Data['鄉鎮市區'].append(y[1])
            Data['交易屋齡'].append(Count_Age(y[8],y[15]))

            if y[14] is None:
                Data['主要建材(鋼骨)'].append(0)
                Data['主要建材(磚造)'].append(0)
                Data['主要建材(其他)'].append(1)
                Data['總計'].append(1)
            elif '鋼' in y[14]:
                Data['主要建材(鋼骨)'].append(1)
                Data['主要建材(磚造)'].append(0)
                Data['主要建材(其他)'].append(0)
                Data['總計'].append(1)
            elif '磚' in y[14]:
                Data['主要建材(鋼骨)'].append(0)
                Data['主要建材(磚造)'].append(1)
                Data['主要建材(其他)'].append(0)
                Data['總計'].append(1)
            else:
                Data['主要建材(鋼骨)'].append(0)
                Data['主要建材(磚造)'].append(0)
                Data['主要建材(其他)'].append(1)
                Data['總計'].append(1)
            
            if y[13] is None:
                Data['主要用途(住家用)'].append(0)
                Data['主要用途(商業用)'].append(0)
                Data['主要用途(工業用)'].append(0)
                Data['主要用途(住商用)'].append(0)
                Data['主要用途(其他)'].append(1)
            elif '住家用' in y[13]:
                Data['主要用途(住家用)'].append(1)
                Data['主要用途(商業用)'].append(0)
                Data['主要用途(工業用)'].append(0)
                Data['主要用途(住商用)'].append(0)
                Data['主要用途(其他)'].append(0)
            elif '商業用' in y[13]:
                Data['主要用途(住家用)'].append(0)
                Data['主要用途(商業用)'].append(1)
                Data['主要用途(工業用)'].append(0)
                Data['主要用途(住商用)'].append(0)
                Data['主要用途(其他)'].append(0)
            elif '工業用' in y[13]:
                Data['主要用途(住家用)'].append(0)
                Data['主要用途(商業用)'].append(0)
                Data['主要用途(工業用)'].append(1)
                Data['主要用途(住商用)'].append(0)
                Data['主要用途(其他)'].append(0)
            elif '住商用' in y[13]:
                Data['主要用途(住家用)'].append(0)
                Data['主要用途(商業用)'].append(0)
                Data['主要用途(工業用)'].append(0)
                Data['主要用途(住商用)'].append(1)
                Data['主要用途(其他)'].append(0)
            else :
                Data['主要用途(住家用)'].append(0)
                Data['主要用途(商業用)'].append(0)
                Data['主要用途(工業用)'].append(0)
                Data['主要用途(住商用)'].append(0)
                Data['主要用途(其他)'].append(1)

#第3步:輸出
Taiwan_Build_DataFrame = pd.DataFrame(Data)
Result = Taiwan_Build_DataFrame.groupby(['所屬縣市','鄉鎮市區','交易屋齡']).sum()
Result.to_csv('test2.csv', encoding='utf_8_sig')


con.close()