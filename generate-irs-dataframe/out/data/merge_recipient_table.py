
import os
import glob
import pandas as pd


path = r'.'
all_files = glob.glob(os.path.join(path, "*.csv"))
names = [os.path.basename(x) for x in glob.glob(path+'\*.csv')]
df = pd.DataFrame()


for file_ in all_files:
    file_df = pd.read_csv(file_, index_col=0, header=0)
    # file_df['file_name'] = file_
    file_df['file_name'] = file_.split("_")[1][:4]
    df = df.append(file_df)
print(df[:50])
print(df.shape)

fp = open(os.path.join(path, 'recipt.csv'), 'w')
df.to_csv(fp,sep=',', index = False)



