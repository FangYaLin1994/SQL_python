import pandas as pd
import sqlite3, csv, os

con = sqlite3.connect('LVR.db') #連結database
cor = con.cursor()

#第1步:從擷取特定資料
TNK_Table_Name = [str(x[0]) for x in cor.execute("select name from 'manifest.csv' where description like '臺北%' or description like '新北%' or description like '基隆%';")]
TNK_Floor_Data = [] #目標資料 
TNK_Floor_Name = [] #資料表名稱
for i in range(len(TNK_Table_Name)):
    check = [x[1] for x in cor.execute("PRAGMA table_info('%s');" % TNK_Table_Name[i])] #確認每個資料表中的欄位名稱
    if '交易筆棟數' in check:
        TNK_Floor_Name.append(TNK_Table_Name[i])

for i in range(len(TNK_Floor_Name)):
        Raw_Data = [x for x in cor.execute("select * from '%s' where 建物型態 like '住宅大樓%%' or 建物型態 like '華廈%%' ;" % TNK_Floor_Name[i])]
        for y in Raw_Data:
            TNK_Floor_Data.append(y)

#第2步:再處理並做成dic
Data = {
    '交易年月':[],
    '總樓層數':[],
    '住家用':[],
    '住商用':[],
    '商業用':[],
    '合計筆數':[]
}
def change2num(word):
    num_dic = {
        '一':1 , '二':2 , '三':3 , '四':4 , '五':5 , '六':6 , '七':7 , '八':8 , '九':9, '十':10
    }
    if word is None:
        return '未知'
    elif '層' in word:
        number = word[:-1] #去掉層
        if len(number) == 1:
            return num_dic[number]
        elif len(number) == 2:
            if num_dic[number[0]] == 10:
                return num_dic[number[0]]+num_dic[number[1]]
            else:
                return num_dic[number[0]]*num_dic[number[1]]
        else:
            return num_dic[number[0]]*num_dic[number[1]]+num_dic[number[2]]
    else:
        return int(word)

def Check_Floor(floor):
    if floor == '未知':
        return floor
    elif floor <= 10:
        return '10以下'
    elif 10 < floor <= 20:
        return '11~20層'
    elif 20 < floor <= 30:
        return '21~30層'
    else:
        return '31以上'

for x in range(len(TNK_Floor_Data)):
    y = TNK_Floor_Data[x] #y[8]交易日期,y[11]樓層,y[13]用途
    date = y[8]  #ex:1081119
    year_month = str(int(y[8])//100) #ex:10811
    if y[13] is None :
        continue
    elif '住家用' in y[13]:
        Data['交易年月'].append(year_month)
        Data['總樓層數'].append(Check_Floor(change2num(y[11])))
        Data['住家用'].append(1)
        Data['住商用'].append(0)
        Data['商業用'].append(0)
        Data['合計筆數'].append(1)
    elif '住商用' in y[13]:
        Data['交易年月'].append(year_month)
        Data['總樓層數'].append(Check_Floor(change2num(y[11])))
        Data['住家用'].append(0)
        Data['住商用'].append(1)
        Data['商業用'].append(0)
        Data['合計筆數'].append(1)
    elif '商業用' in y[13]:
        Data['交易年月'].append(year_month)
        Data['總樓層數'].append(Check_Floor(change2num(y[11])))
        Data['住家用'].append(0)
        Data['住商用'].append(0)
        Data['商業用'].append(1)
        Data['合計筆數'].append(1)


#第3步:輸出
TNK_DataFrame = pd.DataFrame(Data)
Result = TNK_DataFrame.groupby(['交易年月','總樓層數']).sum()
Result.to_csv('test4.csv', encoding='utf_8_sig')

con.close()
