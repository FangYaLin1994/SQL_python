import pandas as pd
import sqlite3, csv, os

con = sqlite3.connect('LVR.db') #建立database
cor = con.cursor()

#第1步:csv匯入DB
for filename in os.listdir('C:/Users/david/HomeWork/Statistic/lvr_landcsv'):
    df = pd.read_csv('C:/Users/david/HomeWork/Statistic/lvr_landcsv/%s' % filename)
    df.to_sql(filename, con, if_exists='replace')

#第2步:從manifest.csv擷取資料
Country_Name_All = [str(x[0]) for x in cor.execute("select description from 'manifest.csv';")] #縣市 ex:台北市
Table_Name_All = [str(x[0]) for x in cor.execute("select name from 'manifest.csv';")]  #資料表 ex:a_lvr_land_a.csv

#第3步:將縣市輸入於資料表中
for i in range(len(Country_Name_All)):
    Country_Name = Country_Name_All[i]
    Table_Name = Table_Name_All[i]
    cor.execute("alter table '%s' add 縣市 text ;" % Table_Name)
    con.commit()
    cor.execute("update '%s' set 縣市 = '%s' ;" % (Table_Name, Country_Name[0:3]))
    con.commit()

'''
#找table的名字
Table_Name = cor.execute("select name from sqlite_master where type = 'table';") 
for x in Table_Name: #print(x[0])
    Check_Column = cor.execute("select 交易筆棟數 from '%s';" % x)
    for line in Check_Column:
        for field in line:
            print(field)
    print()
'''

con.close()