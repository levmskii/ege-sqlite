from gettingdata import datas
import sqlite3
from to_csv import labels

class Databased():
    def __init__(self, baselabe):
        self.base = sqlite3.connect(baselabe)
        self.cur = self.base.cursor()
        self.vpr = 5328
        for i in range(len(labels)):
            count_colums = len(datas[i][0])
            match count_colums:
                case 2:
                    c1, c2 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}) values(?, ?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()
                case 3:
                    c1, c2, c3 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT, {c3} TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}, {c3}) values(?, ?,?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()
                case 4:
                    c1, c2, c3, c4 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT, {c3} TEXT, {c4} TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}, {c3}, {c4}) values(?, ?,?,?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()
                case 5:
                    c1, c2, c3, c4, c5 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT, {c3} TEXT, {c4} TEXT, {c5} TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}, {c3}, {c4}, {c5}) values(?, ?,?,?,?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()
                case 6:
                    c1, c2, c3, c4, c5,c6 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT, {c3} TEXT, {c4} TEXT, {c5} TEXT,{c6} TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}, {c3}, {c4}, {c5}, {c6}) values(?,?,?,?,?,?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()
                case 7:
                    c1, c2, c3, c4, c5,c6,c7 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT, {c3} TEXT, {c4} TEXT, {c5} TEXT,{c6} TEXT,{c7} TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}, {c3}, {c4}, {c5}, {c6}, {c7}) values(?,?,?,?,?,?,?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()
                case 8:
                    c1, c2, c3, c4, c5,c6,c7,c8 = datas[i][0]
                    CREATE_TABLE_QUERY = f'CREATE TABLE IF NOT EXISTS {labels[i]}({c1} TEXT, {c2} TEXT, {c3} TEXT, {c4} TEXT, {c5} TEXT,{c6} TEXT,{c7}TEXT,{c8}TEXT);'
                    self.cur.execute(CREATE_TABLE_QUERY); self.base.commit()
                    INSERT_QUERY = f'INSERT INTO {labels[i]}({c1}, {c2}, {c3}, {c4}, {c5}, {c6},{c7},{c8}) values(?,?,?,?,?,?,?,?)'
                    self.cur.executemany(INSERT_QUERY, datas[i][1:]); self.base.commit()

        while self.vpr:
            vpr = input('\nХотите воспользоваться функцией ВПР?(Да/Нет)\n')
            match vpr:
                case 'Да':
                    try:
                        vpr = input('Введите впр в формате <ВПР(таблцица_для_присоединения, критерий_поиска, таблица_источник, номер_столбца)>\n')
                        vpr = vpr[4:-1].split(', ')
                        self.VPR(vpr[0], vpr[1], vpr[2], vpr[3])
                    except:
                        print("Вы неправиьлно ввели функцию. Внимательно изучите формат...")
                case 'Нет':
                    self.vpr = 0

    def VPR(self, table1, column1, table2, column2_indx):
        table1_indx = labels.index(table1)
        column1_indx = datas[table1_indx][0].index(column1)
        table2_indx = labels.index(table2)
        column2_indx =int(column2_indx)-1
        self.cur.execute(f'DROP TABLE IF EXISTS {table1}')
        columbs = ', '.join([f'{x} TEXT NOT NULL' for x in datas[table1_indx][0]]) + f', {datas[table2_indx][0][column2_indx]} TEXT'
        self.cur.execute(f'CREATE TABLE IF NOT EXISTS {table1} ({columbs})') 
        self.base.commit()

        insert_columbs = ', '.join([x for x in datas[table1_indx][0]] + [datas[table2_indx][0][column2_indx]])
        quests = ', '.join(['?' for x in range(len(datas[table1_indx][0])+1)])
        insertion = f'Insert into {table1}({insert_columbs}) values({quests})'
        new_datas = []
        for i in datas[table1_indx][1:]:
            for j in datas[table2_indx][1:]:
                if i[column1_indx] == j[0]:
                    new_datas.append(i+[j[column2_indx]])
        print(new_datas)
        self.cur.executemany(insertion, new_datas)
        self.base.commit()
        
if __name__ == '__main__':
    labe = open('labels.txt', encoding='UTF-8').readline().rstrip()
    base = Databased(f'{labe}.db')