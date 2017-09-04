import pandas as pd
from get_indices_cv import indices_for_cv
X_train = pd.read_csv('X_train_countJ_gow_final.csv').values
Y_train = pd.read_csv('Y_train_countJ_gow_final.csv').values
X_test = pd.read_csv('X_test_countJ_gow_final.csv').values

#########################
## OPTIMISATION IMPORT ##
#########################
from sklearn.cross_validation import train_test_split
from sklearn import cross_validation
import numpy as np
import xgboost as xgb
import hyperopt
from hyperopt import fmin, tpe, hp,STATUS_OK, Trials
import math
from sklearn.metrics import mean_squared_error, make_scorer

date = 050416

X_train1, X_test1, Y_train1, Y_test1 = cross_validation.train_test_split(X_train,
            Y_train, test_size=0.66)

dtrain = xgb.DMatrix(X_train1, label=Y_train1)
dvalid = xgb.DMatrix(X_test1, label=Y_test1)

watchlist = [(dtrain, 'train'),(dvalid, 'eval')]
num_round = 200
# Construction train test avec indices representatifs
# ind_train, ind_test = indices_for_cv()
# X_train1, y_train1 = X_train[ind_train], Y_train[ind_train]
# X_test1, y_test1 = X_train[ind_test], Y_train[ind_test]

# Tuning des parametres d un xgboost avec hyperopt
print "debut optimisation"

space4rf = {   
           'colsample_bytree' : hp.quniform('colsample_bytree', 0.4, 1., 0.005),
           'gamma' : hp.quniform('gamma', 0., 10., 0.05),
           'learning_rate' : hp.uniform('eta', 0.01, 0.1),
           'max_depth' : hp.choice('max_depth', range(5, 20)),
           'min_child_weight' : hp.quniform('min_child_weight', 0., 6., 1),
           'n_estimators': 500,
           'nthread': 30,
           'objective':'reg:linear',
           'reg_alpha':hp.quniform('reg_alpha', 0.0, 10.0, 0.005),
           'reg_lambda':hp.quniform('reg_lambda', 0.0, 10.0, 0.005),
           'colsample_bylevel' : hp.quniform('subsample', 0.4, 1, 0.05)
           }

best = 0
df_result_hyperopt = pd.DataFrame(columns=[np.append(['score','best iter'], (space4rf.keys()))])
# On stock les resultats dans un dataframe
i = 0


def rmse_score_dmatrix(params):
    # calcul le score pour un set de parametre
    global i    
    print '------------------'
    print i
    print params
    model = xgb.train(params, dtrain, num_round, evals=watchlist, early_stopping_rounds=30, verbose_eval=1)

    print " =============== "
    print model.best_iteration
    predictions = model.predict(dvalid)
    train_pred = model.predict(dtrain)
    loss = math.sqrt(mean_squared_error(predictions, Y_test1))
    train_score = math.sqrt(mean_squared_error(train_pred, Y_train1))
    print "\t Train Score {0}\n".format(train_score)
    print "\t Eval Score {0}\n\n".format(loss)
    df_result_hyperopt.columns=[np.append(['score','best iter'], (params.keys()))]
    df_result_hyperopt.loc[i] = np.append([loss, model.best_iteration], params.values() )
   
    print loss
 
    print '------------------'
    i = i+1
    print 'next'  
    return {'loss': loss, 'status': STATUS_OK}

def rmse_score(params):
    # calcul le score pour un set de parametre
    global i    
    print '------------------'
    print i
    print params
    gbm = xgb.XGBRegressor(**params).fit(X_train1, Y_train1, verbose = 1, eval_metric="rmse",
                                        early_stopping_rounds=20,
                                        eval_set=[(X_test1, Y_test1)])
    print " =============== "
    print gbm.best_iteration
    y_pred_train = gbm.predict(X_test1)
    loss = math.sqrt(mean_squared_error(Y_test1, y_pred_train))
    df_result_hyperopt.columns=[np.append(['score','best iter'], (params.keys()))]
    df_result_hyperopt.loc[i] = np.append([loss, gbm.best_iteration], params.values() )
   
    print loss
 
    print '------------------'
    i = i+1
    print 'next'  
    return {'loss': loss, 'status': STATUS_OK}

trials = Trials()

best = fmin(rmse_score, space4rf, algo=tpe.suggest, max_evals = 50, trials=trials)

print 'best:'
print best
df_result_hyperopt = df_result_hyperopt.sort_values(
   'score', axis = 0, ascending=[True])
df_result_hyperopt.head()
df_result_hyperopt.to_csv('result_hyperoptJ_final.csv')

print "fin opti"

#######################################
########## CROSS VALIDATION ###########
#######################################


print "crossval"


def cross_val (n_folds, n_params) :
    params_cv = df_result_hyperopt.sort_values('score', ascending=True).head(n_params).to_dict(orient='records')[0:n_params]
    tableau_cv = []
    param_tableau_cv = []
    for i in range (int(n_params)) :
        print "training with params", params_cv[i]
        error = 0
        best_iter_sum = 0
        del params_cv[i]['score']
        best_iter_cv = int(float((params_cv[i]['best iter'])))
        del params_cv[i]['best iter']
        params_cv[i]['n_estimators'] = int((float(params_cv[i]['n_estimators'])))
        xgb_model = xgb.XGBRegressor(**params_cv[i])
        
        for k in range(1,int(n_folds)):
            print k
            X_train_cv, X_test_cv, Y_train_cv, Y_test_cv = cross_validation.train_test_split(X_train,
            Y_train, test_size=0.66)
            xgb_model.fit(X_train_cv, Y_train_cv, early_stopping_rounds = 20,eval_set=[(X_test_cv, Y_test_cv)])
            pred_cv = xgb_model.predict(X_test_cv)
            pred_cv[pred_cv>3]=3
            pred_cv[pred_cv<1]=1
            error +=(mean_squared_error(Y_test_cv,pred_cv))**0.5
        error = error/float(n_folds-1)
        tableau_cv.append(error)
        tableau_cv_df = pd.DataFrame(tableau_cv)
        tableau_cv_df.to_csv("tableau_score_cvJ_final.csv")
    return tableau_cv

tableau_crossval = cross_val(3,3)
# print tableau_crossval

def make_sub (param_sub) :
    # construit le fichier submission.csv a partir d'un set de parametres
    id_test = pd.read_csv('test.csv', encoding="ISO-8859-1")['id']
    gbm = xgb.XGBRegressor(**param_sub).fit(X_train, Y_train, verbose = 1)
    final_pred = gbm.predict(X_test)
    final_pred[final_pred>3]=3
    final_pred[final_pred<1]=1

    pd.DataFrame({"id": id_test, "relevance" : final_pred}).to_csv('submission_J'+str(date)+'.csv', index = False)
    return pd.DataFrame({"id": id_test, "relevance" : final_pred})

print "make sub"
best_param = df_result_hyperopt.sort_values('score', ascending=True).head(1).to_dict(orient='records')[0]
# del best_param['Unnamed: 0']
print "best_param score", best_param['score']
del best_param['score']
best_param['n_estimators'] = int(float(best_param['best iter']))
del best_param['best iter']
best_param['nthread']=-1

# make_sub(best_param)

