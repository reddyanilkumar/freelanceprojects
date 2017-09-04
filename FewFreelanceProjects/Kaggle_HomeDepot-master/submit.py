# coding: utf8

import xgboost as xgb
import hyperopt
from hyperopt import fmin, tpe, hp,STATUS_OK, Trials
from sklearn.cross_validation import train_test_split
import math
from sklearn.metrics import mean_squared_error as MS
import pandas as pd
import numpy as np
date = 060416

X_train = pd.read_csv('X_train_countJ.csv')
Y_train = pd.read_csv('Y_train_countJ.csv')
X_test = pd.read_csv('X_test_countJ.csv')
df_result_hyperopt_load = pd.read_csv('result_hyperoptJ.csv')
id_test = pd.read_csv('test.csv', encoding="ISO-8859-1")['id']

best_param_sorted = df_result_hyperopt_load.sort_values('score', ascending=True).head(10).to_dict(orient='records')[0:10]
tableau_score_cv = pd.read_csv("tableau_score_cvJ.csv")
del tableau_score_cv['Unnamed: 0']

def get_top_indices () :
	tableau_score_cv = pd.read_csv("tableau_score_cvJ.csv")
	del tableau_score_cv['Unnamed: 0']
	inter = tableau_score_cv
	inter = inter.as_matrix()
	top_indices = []
	for i in range (len(tableau_score_cv)) :
		indice = np.argmin(inter)
		top_indices.append(indice)
		inter[indice] = 3
	return top_indices

def make_sub (param_sub) :
	print "best_param score sans cv", param_sub['score']
	print "best_param score avec cv", tableau_score_cv['0'][get_top_indices()[0]]
  	gbm = xgb.XGBRegressor(**param_sub).fit(X_train, Y_train)
  	final_pred = gbm.predict(X_test)
  	final_pred[final_pred>3]=3
  	final_pred[final_pred<1]=1
  	pd.DataFrame({"id": id_test, "relevance" : final_pred}).to_csv(str(date)+"submission_simpleJ.csv", index = False)
  	return pd.DataFrame({"id": id_test, "relevance" : final_pred})

def pred_mean_CV (params, number) :
    sum = np.zeros(X_test.shape[0])
    for i in range (number) :
        print i
        print "prediction numero",i
        gbm = xgb.XGBRegressor(**params[i]).fit(X_train, Y_train)
        print "prediction numero", i, "terminee"
        sum = sum + gbm.predict(X_test)
    res_mean = sum/float(number)
    res_mean[res_mean>3]=3
    res_mean[res_mean<1]=1
    pd.DataFrame({"id": id_test, "relevance" : res_mean}).to_csv(str(date)+"submission_meanCV_J"+str(number)+".csv", index = False)
    return res_mean



def pred_mean(params, number) :
    sum = np.zeros(X_test.shape[0])
    for i in range (number) :
        print i
        print "prediction numero",i
        gbm = xgb.XGBRegressor(**params[i]).fit(X_train, Y_train)
        print "prediction numero", i, "terminee"
        sum = sum + gbm.predict(X_test)
    res_mean = sum/float(number)
    res_mean[res_mean>3]=3
    res_mean[res_mean<1]=1
    pd.DataFrame({"id": id_test, "relevance" : res_mean}).to_csv(str(date)+"submission_mean_J"+str(number)+".csv", index = False)
    return res_mean

mesmeilleursparamcv = []
for i in range (len(tableau_score_cv)) :
	mesmeilleursparamcv.append(best_param_sorted[get_top_indices()[i]])
	del mesmeilleursparamcv[i]['score']
	del mesmeilleursparamcv[i]['Unnamed: 0']
	best_iter_final_cv = int(mesmeilleursparamcv[i]["best iter"])
	del mesmeilleursparamcv[i]["best iter"]
	mesmeilleursparamcv[i]["n_estimators"] = best_iter_final_cv

mesmeilleursparam = best_param_sorted = df_result_hyperopt_load.sort_values('score', ascending=True).head(10).to_dict(orient='records')[0:10]
for i in range (len(mesmeilleursparam)) :
	del mesmeilleursparam[i]['score']
	del mesmeilleursparam[i]['Unnamed: 0']
	best_iter_final = int(mesmeilleursparam[i]["best iter"])
	del mesmeilleursparam[i]["best iter"]
	mesmeilleursparam[i]["n_estimators"] = best_iter_final



pred_mean(mesmeilleursparam,3)
pred_mean(mesmeilleursparam,1)

def thresholdsub(sub, thresh) :
    n =3
    values = [1,1.67,2.33,2.5,2.67,3]
    for values in values :
        sub[([sub>value-thresh]) and ([sub<value+thresh])]= value
    return sub