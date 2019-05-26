# -*- coding: utf-8 -*-
# @Time : 2019/4/1 
# @Author : wei cai
# @FileName: topic_lda.py
# @Project: nlp-topic-clustering
import pandas as pd
import numpy as np
import gensim
from gensim.corpora import Dictionary
import os
from pprint import pprint
import pickle
import jieba
import pandas as pd
import tqdm
import sys 
import re
import operator




def kill_unknowns(x):
    if x == "Unknown":
        return " "
    else:
        return x

def kill_unknowns_nan(x):
    if x=="Unknown":
        return float('nan')
    else:
        return x

def kill_unknowns_tf(x):
    if x=="Unknown":
        return False
    else:
        return True

def zip_5(x):

    x=str(x)
    if len(x) <= 4:
        x=x.zfill(5)
    elif len(x)==9:
        x=x[0:5]
    elif len(x) ==8:
        x='0' + x[0:4]
    else:
        x        
    return x

def zip_merge(row):
   

    if row['ReturnHeader_Filer_USAddress_ZIPCode'] == row['ReturnData_IRS990_USAddress_ZIPCd']:
        val = row['ReturnHeader_Filer_USAddress_ZIPCode']
    elif row['ReturnHeader_Filer_USAddress_ZIPCode'] == ' ' and row['ReturnData_IRS990_USAddress_ZIPCd'] != ' ':
        val = row['ReturnData_IRS990_USAddress_ZIPCd']
    else:
        val = row['ReturnHeader_Filer_USAddress_ZIPCode']
    return val



def concat(x):
    col1 = x['TAXPAYER_NAME']
    col2 = x['ReturnData_IRS990_MissionDesc_new']
    col3 = x['ReturnData_IRS990_ActivityOrMissionDesc_new']
    return col1 + " " + col2 + " " + col3


def combine(inde, rec):
    inde = inde.rename(columns={'ZIPCd': 'zipsrc'})
    inde['zipsrc']=inde['zipsrc'].apply(zip_5)
    rec = rec.rename(columns={'USAddress_ZIPCd': 'ziptgt'})

    a = inde[["zipsrc", "OBJECT_ID", "CharityGroup"]]

    b = rec[["ziptgt", "CashGrantAmt","OBJECT_ID"]]
    result = pd.merge(left=a, right=b, left_on="OBJECT_ID", right_on="OBJECT_ID")


def get_tf(text):
    text = str(text).lower()
    # remove tags
    text = re.sub("&lt;/?.*?&gt;", " &lt;&gt; ", text)
    # remove special characters and digits
    text = re.sub("(\\d|\\W)+", " ", text)
    return text


def f(row):

    # if row['USAddress_ZIPCode'] != 'Unknown' and row['USAddress_ZIPCd'] != 'Unknown' and row['AddressUS_ZIPCode'] != 'Unknown':
    #     val = row['USAddress_ZIPCode']
   
    if row['USAddress_ZIPCd'] != ' ' or row['USAddress_ZIPCd'] != 0 :
        val = row['USAddress_ZIPCd']
    elif row['AddressUS_ZIPCode'] != ' ' or  row['AddressUS_ZIPCode'] != 0:
        val = row['AddressUS_ZIPCode']
    elif row['USAddress_ZIPCode'] != ' ' or row['USAddress_ZIPCode'] != 0:
        val = row['USAddress_ZIPCode']
    # elif row['USAddress_ZIPCode'] != 'Unknown' and row['USAddress_ZIPCd'] != 'Unknown' and row['AddressUS_ZIPCode'] != 'Unknown':
    #     val = row['USAddress_ZIPCode']
    # else:
    #     val = 'Unknown'
    return val

def s(row):
    # if row['ReturnHeader_Filer_USAddress_ZIPCode'] != 'Unknown' and row['ReturnData_IRS990_USAddress_ZIPCd'] != 'Unknown':
    #     val = row['ReturnHeader_Filer_USAddress_ZIPCode']
    if row['ReturnHeader_Filer_USAddress_ZIPCode'] != ' ':
        val = row['ReturnHeader_Filer_USAddress_ZIPCode']
    else:
        val = row['ReturnData_IRS990_USAddress_ZIPCd']
    # elif row['ReturnHeader_Filer_USAddress_ZIPCode'] != 'Unknown' and row['ReturnData_IRS990_USAddress_ZIPCd'] != 'Unknown':
    #     val = row['ReturnHeader_Filer_USAddress_ZIPCode']
    
    return val




def c(row):
    # if row['AmountOfCashGrant'] != 'Unknown' and row['CashGrantAmt'] != 'Unknown':
    #     val = row['AmountOfCashGrant']
    if row['CashGrantAmt'] != ' ':
        val = row['CashGrantAmt']
    elif row['AmountOfCashGrant'] != ' ':
        val = row['AmountOfCashGrant']
    
    # else:
    #     val = 'Unknown'
    return val


def tokenize(corpus):
    stopwords_dir = r'stopwords.txt'
    with open(stopwords_dir, 'r') as f:
        stopwords = [eachWord.strip().lower() for eachWord in f.readlines() if eachWord.strip() != '']
        new_stopwords = (' ','','organization','foundation','s','county','support','mission','charitable','public','services','service','educational','promote','community')
        for i in new_stopwords:
            stopwords.append(i)
    stopwords.append(' ')
    stopwords.append('')
    stopwords.append('organization')
    stopwords.append('foundation')
    stopwords.append('s')
    stopwords.append('county')
    stopwords.append('support')
    stopwords.append('mission')
    stopwords.append('public')
    stopwords.append('services')
    stopwords.append('service')
    stopwords.append('educational')
    stopwords.append('promote')
    stopwords.append('community')
  


    # cut words
    wordList = []
    for i_str in corpus:
        a = list(jieba.cut(i_str.strip()))
        b = []
        for ele in a:
            if ele.strip().lower() not in stopwords: b.append(ele.strip().lower())
        wordList.append(b)

    # Dictionary
    my_label_dictionary = Dictionary(wordList)
    # bow
    bow_corpus = [my_label_dictionary.doc2bow(pdoc) for pdoc in wordList]

    return bow_corpus, my_label_dictionary



work_dir = "./"
# if len(sys.argv) != 2:
#     print("Usage: {} 2011/2012/2013/2014/2015/2016".format(sys.argv[0]))
#     sys.exit(1)
# input_type = sys.argv[1].strip()
# if input_type not in ["2011", "2012", "2013", "2014","2015","2016"]:
#     print("Unknown input {}".format(input_type))
#     sys.exit(1)


def main():


    df = pd.read_csv("index.csv", sep=',', low_memory=False)
    
    # df = df[:130000]
    # df = df[130001:]
    # cleaning
    df = df.fillna(' ')
    df = df.applymap(kill_unknowns)


    # maybe redundant just in case 
    df = df[df.ReturnData_IRS990_MissionDescription != 'Unknown']
    df = df[df.ReturnData_IRS990_MissionDesc != 'Unknown']

    df = df[df.ReturnData_IRS990_ActivityOrMissionDescription != 'Unknown']
    df = df[df.ReturnData_IRS990_ActivityOrMissionDesc != 'Unknown']

    # df= df[df.ReturnHeader_Filer_USAddress_ZIPCode != 'Unknown']
    # df= df[df.ReturnData_IRS990_USAddress_ZIPCd != 'Unknown']

    df['ReturnData_IRS990_MissionDesc_new']=df.ReturnData_IRS990_MissionDescription + df.ReturnData_IRS990_MissionDesc
    df['ReturnData_IRS990_MissionDesc_new'].replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # dropping passed values 
    # df.drop(["ReturnData_IRS990_MissionDescription", "ReturnData_IRS990_MissionDesc"], inplace = True) 

    df['ReturnData_IRS990_ActivityOrMissionDesc_new']=df.ReturnData_IRS990_ActivityOrMissionDescription + df.ReturnData_IRS990_ActivityOrMissionDesc
    df['ReturnData_IRS990_ActivityOrMissionDesc_new'].replace(r'^\s*$', np.nan, regex=True, inplace=True)
    
    df['ZIPCd'] = df.apply(s, axis=1)
    df['ZIPCd']=df['ZIPCd'].apply(zip_5)

   

   

    #drop duplicate columns (tagged by .1)
    for col in df.columns:
        if '.1' in col:
            df.drop(col,axis=1)

    # df=df[["OBJECT_ID","ZIPCd","ReturnData_IRS990_MissionDesc", "TAXPAYER_NAME",
    #                           "ReturnData_IRS990_ActivityOrMissionDesc"]]
    
    # df['row_text'] = df[['ReturnData_IRS990_MissionDesc', 'TAXPAYER_NAME',
    #                           'ReturnData_IRS990_MissionDesc']].apply(lambda row: '%s_%s_%s'.join(row), axis=1)
    
    
    df['row_text'] = df[['ReturnData_IRS990_MissionDesc_new', 'TAXPAYER_NAME',
                              'ReturnData_IRS990_ActivityOrMissionDesc_new']].apply(lambda row: ' '.join(str(r) for r in row) , axis=1)
    
    df_main = df.copy()

    lda_model_dir = 'lda-ckp/model.lda'
    label_dictionary_dir = 'lda-ckp/label_dictionary.pk'
    corpus_dir = 'lda-ckp/corpus.pk'
    bow_corpus_dir = 'lda-ckp/bow_corpus.pk'

    lda_model = None
    bow_corpus = None
    corpus = None
    my_label_dictionary = None

    N_WORKERS=4

    # asset restore
    if os.path.exists(bow_corpus_dir):
        print("asset restore from save")
        print()

        output_label_dictionary = open(label_dictionary_dir, 'rb')
        output_bow_corpus = open(bow_corpus_dir, 'rb')
        output_corpus = open(corpus_dir, 'rb')
        my_label_dictionary = pickle.load(output_label_dictionary)
        bow_corpus = pickle.load(output_bow_corpus)
        corpus = pickle.load(output_corpus)
        output_label_dictionary.close()
        output_bow_corpus.close()
        output_corpus.close()
    else:
        print("corpus preparing")
        corpus = df_main['row_text'].apply(get_tf).tolist()
        print("corpus bowing")
        bow_corpus, my_label_dictionary = tokenize(corpus)

        output_label_dictionary = open(label_dictionary_dir, 'wb')
        output_bow_corpus = open(bow_corpus_dir, 'wb')
        output_corpus = open(corpus_dir, 'wb')
        pickle.dump(my_label_dictionary, output_label_dictionary)
        pickle.dump(bow_corpus, output_bow_corpus)
        pickle.dump(corpus, output_corpus)
        output_label_dictionary.close()
        output_bow_corpus.close()
        output_corpus.close()

    # lda restore
    if os.path.exists(lda_model_dir):
        print("lda restore from save")
        print()
        lda_model = gensim.models.LdaModel.load(lda_model_dir)
    else:
        print("LDA model training")
        # lda_model = gensim.models.LdaModel(bow_corpus, iterations=2000, num_topics=8, id2word=my_label_dictionary)
        lda_model = gensim.models.ldamulticore.LdaMulticore(bow_corpus, iterations=2000, num_topics=9, id2word=my_label_dictionary, workers=N_WORKERS)
        lda_model.save(lda_model_dir)

    print("lda model prepared")
    print()

    
    topic_lists = []
    for k in tqdm.tqdm(range(len(corpus)), total=len(corpus)):  # range(3):#
        # print()
        # print(lda_model[bow_corpus[k]])
        # print(corpus[k])

        for index, score in sorted(lda_model[bow_corpus[k]], key=lambda tup: -1 * tup[1]):
            topic, v = lda_model.show_topic(index, 1)[0]
            # topic = lda_model.print_topic(index, 5)
            # a_score = score
            # topic_index.append(topic)
            # score_index.append(a_score)
            # print(index, topic, a_score)
            topic_lists.append(topic)
            break


    asa = list(set(topic_lists))
    print(asa)
    print(len(asa))
    # exit(1)

    # df_origin = pd.read_csv('../index_2016990_plus_xml_copy.csv', sep=',',
    #                         usecols=["ReturnData_IRS990_ActivityOrMissionDesc", "TAXPAYER_NAME",
    #                                  "ReturnData_IRS990_MissionDesc"], low_memory=False)

    df_main['CharityGroup'] = pd.Series(topic_lists)

    df_main = df_main.rename(columns={'file_name': 'Iyear'})
    # df_main.drop(["Unnamed: 0"], inplace = True) 
    # df_origin['CharityGroup'] = topic_lists
    # df_main['CharityGroup'].fillna(' ')
    # df_main['CharityGroup'].replace(r'\s+', 'unknown', regex=True)

    # df_main.to_csv('DESC-lda.csv', index=False)
    # df_origin.to_csv('index_2016990_plus_xml-lda.csv', index=False)
    print(df_main.head(20))
    print(df_main.shape)
    print(df_main.dtypes)
    
    #read and process recipient table 

    # recipients=pd.read_csv('./RecipientTable_2016990.csv', sep=',', usecols=["OBJECT_ID","USAddress_ZIPCd","CashGrantAmt"], low_memory=False)
    
    recipients = pd.read_csv("recipt.csv", sep=',',low_memory=False)

    recipients= recipients.rename(columns={'file_name': 'Ryear'})
    # recipients=recipients[:990]


  

    # recipients['CashGrantAmt']=recipients['CashGrantAmt'].astype(np.float32)
    # recipients = recipients[recipients['CashGrantAmt'] >= 0]

    recipients['CashGrantAmt0'] = recipients.apply(c, axis=1)
    recipients= recipients[~recipients['CashGrantAmt0'].str.contains('Unknown')]
    recipients['CashGrantAmt0']=recipients['CashGrantAmt0'].astype(np.float32)
    recipients = recipients[recipients['CashGrantAmt0'] >= 0]


    recipients['RZIPCd'] = recipients.apply(f, axis=1)
    recipients['RZIPCd']=recipients['RZIPCd'].apply(zip_5)
    # print(recipients['RZIPCd'][:50])
    # exit(1)

    
    print("recipients shape:  ")
    print(recipients.shape[0])

   
    recipients['CashGrantAmt0']=recipients['CashGrantAmt0'].apply(kill_unknowns_nan)
    
    recipients_df=recipients.copy()

    print("recipients df shape:  ")
    print(recipients_df.shape[0])
    

    # recipients_df = recipients_df[pd.notnull(recipients_df['RZIPCd'])]
    recipients_df = recipients_df.rename(columns={'RZIPCd': 'zip_tgt'})
    recipients_df['zip_tgt']=recipients_df['zip_tgt'].apply(zip_5)
    # recipients['zip_tgt']=recipients['zip_tgt'].apply(kill_unknowns_nan)
    # recipients_df.to_csv("./temp/recipt_clean.csv", sep=",", index=False)
    print(recipients_df)

    

    df_main = df_main.rename(columns={'ZIPCd': 'zip_src'})
    df_main['zip_src']=df_main['zip_src'].apply(zip_5)

    # df_main['zip_src']=df_main['zip_src'].apply(kill_unknowns_nan)
    
    #join df_main with recipients_df 

    # df_main['OBJECT_ID']=df_main['OBJECT_ID'].astype('int64')
    # recipients_df['OBJECT_ID']=recipients_df['OBJECT_ID'].astype('int64')
    df_main['OBJECT_ID']=df_main['OBJECT_ID'].astype('int64')
    recipients_df['OBJECT_ID']=recipients_df['OBJECT_ID'].astype('int64')

    print("df_main OBJECT_ID: ")
    print(df_main["OBJECT_ID"])

    print("recipients_df OBJECT_ID: ")
    print(recipients_df["OBJECT_ID"])
    
    print("recipients_df: ")
    print(recipients_df[:5])

    result = pd.merge(left=df_main, right=recipients_df, left_on=['OBJECT_ID','Iyear'],right_on=['OBJECT_ID','Ryear'])

    print('number of rows in the joined file:')
    print(result.shape[0])
    

    print("result df: ")
    print(result[:5])
    

    combo_df=result.copy()

    print('combo df data')
    print(combo_df.head())

    print('***********count number of rows by year for Ryear********')
    print(combo_df.groupby(['Ryear']).size().reset_index(name='counts'))
    
   
 
    #drop columns with all nulll values and drop columns that are not needed 


    combo_df= combo_df[~combo_df['zip_src'].str.contains('Unkno')]
    combo_df= combo_df[~combo_df['zip_src'].str.contains('nan')]

    combo_df= combo_df[~combo_df['zip_tgt'].str.contains('Unkno')]
    combo_df= combo_df[~combo_df['zip_tgt'].str.contains('nan')]


    combo_df['zip_src'] =combo_df['zip_src'].convert_objects(convert_numeric=True)
    combo_df = combo_df[np.isfinite((combo_df['zip_src']))]
    combo_df['zip_src']= combo_df.zip_src.astype(int).astype(str).str.zfill(5)

    
    combo_df['zip_tgt'] =combo_df['zip_tgt'].convert_objects(convert_numeric=True) 
    combo_df = combo_df[np.isfinite((combo_df['zip_tgt']))] 
    combo_df['zip_tgt']= combo_df.zip_tgt.astype(int).astype(str).str.zfill(5)

    combo_df.to_csv("./temp/combo_clean.csv", sep=",", index=False)

    print("number of rows in combo_df: ")
    print(combo_df.shape[0])

    # exit(1)

    # combo_df['zip_src_tgt'] = combo_df['zipsrc'].map(str) + combo_df['ziptgt'].map(str) 
    
    combo_df=combo_df[['zip_src','zip_tgt','CharityGroup','CashGrantAmt0','Ryear']]

    combo_df= combo_df.rename(columns={'CashGrantAmt0': 'CashGrantAmt'})

    combo_df = combo_df.rename(columns={'Ryear': 'year'})

    agg_grant_flow=combo_df.groupby(['zip_src','zip_tgt','year'],as_index=False).sum()
    agg_grant_flow = agg_grant_flow[agg_grant_flow.CashGrantAmt != 0]
    agg_grant_flow.to_csv("./temp/index_agg_grant_flow.csv", index=False)
    agg_grant_flow.to_json('./temp/index_agg_grant_flow.json', orient='records', lines=True)

    print('***********count number of rows by year for agg_grant_flow********')
    print(agg_grant_flow.groupby(['year']).size().reset_index(name='counts'))
    
    agg_grant_group_flow=combo_df.groupby(['zip_src','zip_tgt','CharityGroup','year'],as_index=False).sum()
    agg_grant_group_flow = agg_grant_group_flow[agg_grant_group_flow.CashGrantAmt != 0]
    agg_grant_group_flow.to_csv("./temp/index_agg_grant_group_flow.csv", index=False)
    agg_grant_group_flow.to_json('./temp/index_agg_grant_group_flow.json', orient='records', lines=True)


if __name__ == '__main__':
    main()
