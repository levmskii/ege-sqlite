from to_csv import labels

datas = []
for i in range(len(labels)):
    datas.append([])

for i in range(len(labels)):
    with open(f'{labels[i]}.csv', encoding='UTF-8') as f:
        for j in f.readlines(): 
            line = j.replace(' ', '_').replace('.','').replace('/','').replace(',','').replace('"', '').split('|')
            datas[i].append(line[:len(line)-1]+[line[-1][:-1].strip()])
            
          
