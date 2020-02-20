import pandas as pd
import sqlite3, csv, os

con = sqlite3.connect('LVR.db') #連結database
cor = con.cursor()

#第1步:從manifest.csv擷取臺北、新北、基隆資料表名稱
TNK_Table_Name = [str(x[0]) for x in cor.execute("select name from 'manifest.csv' where description like '臺北%' or description like '新北%' or description like '基隆%';")]

#第2步:整理出所需資料並建立dic
Data = {
    '交易日期':[],
    '主要用途':[],
    '公寓交易筆數':[],
    '華廈交易筆數':[],
    '套房交易筆數':[],
    '住宅大樓交易筆數':[],
    '其他交易筆數':[],
    '成交總數':[],
    '公寓交易總價':[],
    '華廈交易總價':[],
    '套房交易總價':[],
    '住宅大樓交易總價':[],
    '其他交易總價':[],
    '成交總價':[]
}
def Use_of_House(use):
    if use is None:
        return '其他'
    elif '住' in use:
        return use
    elif '業' in use:
        return use
    else:
        return '其他'

for i in range(len(TNK_Table_Name)):
    check = [x[1] for x in cor.execute("PRAGMA table_info('%s');" % TNK_Table_Name[i])] #確認每個資料表中的欄位名稱
    if '交易筆棟數' in check:
        Raw_Data = [x for x in cor.execute("select * from '%s' where not (鄉鎮市區 = 'The villages and towns urban district');" % TNK_Table_Name[i])]
        for y in Raw_Data: #y[8]交易日,y[13]用途,y[12]型態,y[22]金額
            date = str(int(y[8])//100)
            Data['交易日期'].append(date)
            Data['主要用途'].append(Use_of_House(y[13]))

            if y[12] is None:
                Data['公寓交易筆數'].append(0)
                Data['華廈交易筆數'].append(0)
                Data['套房交易筆數'].append(0)
                Data['住宅大樓交易筆數'].append(0)
                Data['其他交易筆數'].append(1)
                Data['成交總數'].append(1)
                Data['公寓交易總價'].append(0)
                Data['華廈交易總價'].append(0)
                Data['套房交易總價'].append(0)
                Data['住宅大樓交易總價'].append(0)
                Data['其他交易總價'].append(y[22])
                Data['成交總價'].append(y[22])
            elif '公寓' in y[12]:
                Data['公寓交易筆數'].append(1)
                Data['華廈交易筆數'].append(0)
                Data['套房交易筆數'].append(0)
                Data['住宅大樓交易筆數'].append(0)
                Data['其他交易筆數'].append(0)
                Data['成交總數'].append(1)
                Data['公寓交易總價'].append(y[22])
                Data['華廈交易總價'].append(0)
                Data['套房交易總價'].append(0)
                Data['住宅大樓交易總價'].append(0)
                Data['其他交易總價'].append(0)
                Data['成交總價'].append(y[22])
            elif '華廈' in y[12]:
                Data['公寓交易筆數'].append(0)
                Data['華廈交易筆數'].append(1)
                Data['套房交易筆數'].append(0)
                Data['住宅大樓交易筆數'].append(0)
                Data['其他交易筆數'].append(0)
                Data['成交總數'].append(1)
                Data['公寓交易總價'].append(0)
                Data['華廈交易總價'].append(y[22])
                Data['套房交易總價'].append(0)
                Data['住宅大樓交易總價'].append(0)
                Data['其他交易總價'].append(0)
                Data['成交總價'].append(y[22])
            elif '套房' in y[12]:
                Data['公寓交易筆數'].append(0)
                Data['華廈交易筆數'].append(0)
                Data['套房交易筆數'].append(1)
                Data['住宅大樓交易筆數'].append(0)
                Data['其他交易筆數'].append(0)
                Data['成交總數'].append(1)
                Data['公寓交易總價'].append(0)
                Data['華廈交易總價'].append(0)
                Data['套房交易總價'].append(y[22])
                Data['住宅大樓交易總價'].append(0)
                Data['其他交易總價'].append(0)
                Data['成交總價'].append(y[22])
            elif '住宅大樓' in y[12]:
                Data['公寓交易筆數'].append(0)
                Data['華廈交易筆數'].append(0)
                Data['套房交易筆數'].append(0)
                Data['住宅大樓交易筆數'].append(1)
                Data['其他交易筆數'].append(0)
                Data['成交總數'].append(1)
                Data['公寓交易總價'].append(0)
                Data['華廈交易總價'].append(0)
                Data['套房交易總價'].append(0)
                Data['住宅大樓交易總價'].append(y[22])
                Data['其他交易總價'].append(0)
                Data['成交總價'].append(y[22])
            else :
                Data['公寓交易筆數'].append(0)
                Data['華廈交易筆數'].append(0)
                Data['套房交易筆數'].append(0)
                Data['住宅大樓交易筆數'].append(0)
                Data['其他交易筆數'].append(1)
                Data['成交總數'].append(1)
                Data['公寓交易總價'].append(0)
                Data['華廈交易總價'].append(0)
                Data['套房交易總價'].append(0)
                Data['住宅大樓交易總價'].append(0)
                Data['其他交易總價'].append(y[22])
                Data['成交總價'].append(y[22])

#第3步:輸出
TNK_DataFrame = pd.DataFrame(Data)
Result = TNK_DataFrame.groupby(['交易日期','主要用途']).sum()
Result.to_csv('test3.csv', encoding='utf_8_sig')

con.close()