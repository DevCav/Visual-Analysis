#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 16:20:11 2019

@author: danielsexton
"""
#Dataset from United States Census Bureau, ZIP Code Business Statistics 2016
from sklearn.mixture import GaussianMixture
from sklearn.cluster import KMeans
from sklearn.model_selection import cross_val_predict
import pandas as pd
import numpy as np
import random
import os

random.seed(2)

os.chdir("dta/")
zip_cluster = pd.read_csv('BP_2016_00CZ1.csv',dtype={'GEO.id2': object})
zip_cluster=zip_cluster.fillna(1)
zip_cluster=zip_cluster[['GEO.id2','ESTAB','EMP','PAYANN']]
zip_cluster['PAYANN/ESTAB']=zip_cluster['PAYANN']/zip_cluster['ESTAB']
zip_cluster['PAYANN/EMP']=zip_cluster['PAYANN']/zip_cluster['EMP']
zip_cluster=zip_cluster.replace([np.inf, -np.inf], np.nan)
zip_cluster=zip_cluster.fillna(0)
#kmeans = KMeans(n_clusters=3, random_state=0)

cv_results=cross_val_predict(GaussianMixture(n_components=3),zip_cluster,cv=10)
zip_cluster = pd.concat([zip_cluster, pd.DataFrame(cv_results,columns=["cluster"])], axis=1)

zip_cluster=zip_cluster.rename(columns={"GEO.id2":'zip'})

os.chdir("../out/data/")
zip_cluster.to_csv('zip_cluster.csv');
print(zip_cluster.groupby(['cluster']).mean())
