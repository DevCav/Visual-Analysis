#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 17 14:04:49 2019

@author: danielsexton
"""
import pandas as pd
import numpy as np
import os
from multiprocessing import Pool, cpu_count
import dask.dataframe as dd

def applyParGroupDict(df_group,func):
    val={x:0 for x in np.array(data_samp['zip_tgt'])}
    m={x:{'count':0,'assoc':val.copy()} for x in np.array(data_samp['zip_tgt'])}
    with Pool(cpu_count(),maxtasksperchild=100) as p:
        iout=p.imap(func,[group for _,group in df_group])
        for out in iout:
            for x in out.keys():
                m[x]['count']+=out[x]['count']
                for y in out.keys():
                    m[x]['assoc'][y]+=out[x]['assoc'][y]
    p.close()
    return m

def applyParGroup(df_group,func):
    with Pool(cpu_count(),maxtasksperchild=10) as p:
        out=p.map(func,[[group,matrix] for _,group in df_group])
    p.close()
    return pd.concat(out)

def applyParGroup2(df_group,func):
    with Pool(cpu_count(),maxtasksperchild=10) as p:
        out=p.map(func,[group for _,group in df_group])
    p.close()
    return pd.concat(out).reset_index().drop(['index'],axis=1)

def applyPar(array,func):
    x=[array[item] for item in array.keys()]
    with Pool(cpu_count(),maxtasksperchild=100) as p:
        out=p.map(func,[[name,array[name]] for name in array.keys()])
    p.close()
    return {item1:assoc for item1,assoc in out}

def basket_to_matrix(basket):
    print(basket)
    basket=basket.reset_index()
    m={x:{'count':0,'assoc':val.copy()} for x in np.array(basket['zip_tgt'])}
    for item1 in basket.iterrows(): #for every dollar spent on one zip code
        m[item1[1]['zip_tgt']]['count']=1
        for item2 in basket.iterrows():
             m[item1[1]['zip_tgt']]['assoc'][item2[1]['zip_tgt']]=item2[1]['CashGrantAmt']/item1[1]['CashGrantAmt'] #how many dollars did you spend on another zip code
    del basket
    del item1
    del item2
    return m

def normalize_matrix(x):
    name=x[0]
    item1=x[1]
    del x
    for item2 in item1['assoc'].keys():
        item1['assoc'][item2]=item1['assoc'][item2]/item1['count'] #normalize to a synthetic profile
    del item2
    m=item1['assoc']
    return name,m
        
def make_synth(x):
    basket=x[0]
    m=x[1]
    s=pd.DataFrame(columns=['zip_tgt','zip_tgt_synth','flow_synth'])
    for item1 in basket.iterrows():
        synth_temp=pd.DataFrame.from_dict(m[item1[1]['zip_tgt']],orient='index').reset_index()
        synth_temp.rename(columns={'index':'zip_tgt_synth',0:'flow_synth'},inplace=True)
        synth_temp['zip_tgt']=str(item1[1]['zip_tgt'])
        s=s.append(synth_temp)
    s=s[s.flow_synth>=s.flow_synth.quantile(.99)]
    del synth_temp
    del item1
    del basket
    del m
    return s

def sum_df_grouped(d):
    return d.groupby(['zip_src','zip_tgt_synth']).sum()

if __name__=="__main__":
    print(os.getcwd())
    os.chdir("out/data/")
    data=pd.read_csv('index_agg_grant_flow.csv',dtype={'zip_src':str,'zip_tgt':str,'CashGrantAmt':int})
    data=data[data['zip_src']!='00000'].reset_index(drop=True)
    data=data[data['year']!='2016'].reset_index()
    
    data_group=data.groupby('zip_src')
    data_samp = [[np.random.uniform(),group] for _,group in data_group]
    data_samp = [group[1] for group in data_samp if group[0]<=0.01 and len(group[1])<=1000]
    data_samp=pd.concat(data_samp).reset_index(drop=True)
    del data_group
    val={x:0 for x in np.array(data_samp['zip_tgt'])}
    matrix={x:{'count':0,'assoc':val.copy()} for x in np.array(data_samp['zip_tgt'])}

    matrix=applyParGroupDict(data_samp.groupby('zip_src'),basket_to_matrix)
    print('basket to matrix done')
    matrix=applyPar(matrix,normalize_matrix)
    print('normalized matrix done')
    synth=applyParGroup(data_samp.groupby('zip_src'),make_synth)
    print('make synth done')

    synth=synth[['zip_tgt','zip_tgt_synth','flow_synth']]
    print('synth cols dropped')

    data=dd.merge(data,synth,on='zip_tgt',how='left')
    print('data merged')

    data=applyParGroup2(data.groupby(['zip_src','zip_tgt_synth']),sum_df_grouped)
    print('data summed')

    data=data[data.flow_synth!=0]
    
    data.to_csv('synth_2016.csv')
    print('data written!')
