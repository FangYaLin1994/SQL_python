import pandas as pd
import sqlite3, csv, os

con = sqlite3.connect('LVR.db') #連結database
cor = con.cursor()

#第1步:從manifest.csv擷取資料
Table_Name_All = [str(x[0]) for x in cor.execute("select name from 'manifest.csv';")]  #資料表 ex:a_lvr_land_a.csv

#第2步:整理出所需資料並建立dic
Data = {
    '所屬縣市':[],
    '所屬地區':[],
    '無車位總數':[],
    '有車位總數':[],
    '車位類別(坡道平面)':[],
    '車位類別(塔式車位)':[],
    '車位類別(升降機械)':[],
    '車位類別(其他)':[],
    '車位總計':[]
}
for i in range(len(Table_Name_All)):
    check = [x[1] for x in cor.execute("PRAGMA table_info('%s');" % Table_Name_All[i])] #確認每個資料表中的欄位名稱
    if '交易筆棟數' in check:
        Raw_Data = [x for x in cor.execute("select * from '%s' where not (鄉鎮市區 = 'The villages and towns urban district');" % Table_Name_All[i])] 
        for y in Raw_Data:   #y[29],y[1],y[9],y[24]
            Trade_Count = y[9]
            Park_Trade_Count =  int(Trade_Count[-1])
            Data['所屬縣市'].append(y[29])
            Data['所屬地區'].append(y[1])
            if Park_Trade_Count == 0:
                Data['無車位總數'].append(1)
                Data['有車位總數'].append(0)
                Data['車位類別(坡道平面)'].append(0)
                Data['車位類別(塔式車位)'].append(0)
                Data['車位類別(升降機械)'].append(0)
                Data['車位類別(其他)'].append(0)
                Data['車位總計'].append(1)
            else:
                Data['無車位總數'].append(0)
                Data['有車位總數'].append(Park_Trade_Count)
                Data['車位總計'].append(Park_Trade_Count)
                if y[24] == '坡道平面':
                    Data['車位類別(坡道平面)'].append(Park_Trade_Count)
                    Data['車位類別(塔式車位)'].append(0)
                    Data['車位類別(升降機械)'].append(0)
                    Data['車位類別(其他)'].append(0)
                elif y[24] == '塔式車位':
                    Data['車位類別(坡道平面)'].append(0)
                    Data['車位類別(塔式車位)'].append(Park_Trade_Count)
                    Data['車位類別(升降機械)'].append(0)
                    Data['車位類別(其他)'].append(0)
                elif y[24] == '升降機械':
                    Data['車位類別(坡道平面)'].append(0)
                    Data['車位類別(塔式車位)'].append(0)
                    Data['車位類別(升降機械)'].append(Park_Trade_Count)
                    Data['車位類別(其他)'].append(0)
                else:
                    Data['車位類別(坡道平面)'].append(0)
                    Data['車位類別(塔式車位)'].append(0)
                    Data['車位類別(升降機械)'].append(0)
                    Data['車位類別(其他)'].append(Park_Trade_Count)

#第3步:輸出
Taiwan_Park_DataFrame = pd.DataFrame(Data)
Result = Taiwan_Park_DataFrame.groupby(['所屬縣市','所屬地區']).sum()
Result.to_csv('test1.csv', encoding='utf_8_sig')

con.close()