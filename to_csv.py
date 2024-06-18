import pandas as pd
from os import path
labels = open(path.abspath('labels.txt'), encoding='UTF-8').readlines()[1].split(',')
exl = input('Укажите название книги: ')
f = pd.read_excel(path.abspath(exl), sheet_name=list(range(len(labels))))
for frame in f.keys():
    f[frame].to_csv(f'{labels[frame]}.csv', sep='|', index=False)