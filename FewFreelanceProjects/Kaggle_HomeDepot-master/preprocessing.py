# coding: utf8 #

#############################
####### SCRIPT IMPORT #######
#############################

from dictionnary import get_dic
import ngramk
from feat_utils import try_divide
import networkx as nx
from sys import getsizeof
import gc
############################
###### GENERAL IMPORT ######
############################

import requests
import re
import time
import pandas as pd
import numpy as np
import random
from random import randint

####################
## SKLEARN IMPORT ##
####################
from sklearn import cross_validation
from sklearn import pipeline, grid_search
from sklearn.ensemble import RandomForestRegressor
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import FeatureUnion
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import mean_squared_error, make_scorer
from sklearn.metrics.pairwise import cosine_similarity, pairwise_distances
from sklearn.feature_extraction import DictVectorizer


#################
## NLTK IMPORT ##
#################
import nltk
from nltk.stem.porter import *


##############################################
#### DEFINE LIST OF STOPWORDS AND NUMBERS ####
##############################################
# stop_w = ['for', 'xbi', 'and', 'in', 'th','on','sku','with','what','from','that','less','er','ing']
stop_w = pd.read_csv("english")
strNum = {'zero':0,'one':1,'two':2,'three':3,'four':4,'five':5,'six':6,'seven':7,'eight':8,'nine':9}


start_time = time.time()
stemmer = PorterStemmer()
random.seed(2016)


################################################################
################## CREATING METHODS ############################
###############################################################

def str_whole_word(str1, str2, i_):
    cnt = 0
    while i_ < len(str2):
        i_ = str2.find(str1, i_)
        if i_ == -1:
            return cnt
        else:
            cnt += 1
            i_ += len(str1)
    return cnt

def str_operation_cleaning(s): 
    s = s.replace(" x "," xby ")
    s = s.replace("*"," xby ")
    s = s.replace(" by "," xby")
    s = re.sub(r"([0-9]+)(x|')\.?", r"\1 xby ", s)
    s = re.sub(r"(x|')([0-9]+)", r"xby \1", s)
    s = re.sub(r"([0-9]+)([a-z])", r"\1 \2", s)
    return s

def str_stem(s): 
    s = str(s)
    if isinstance(s, str):
        s = re.sub(r"(\w)\.([A-Z])", r"\1 \2", s)
        s = s.lower()
        s = re.sub(r"(\w)\.([A-Z])", r"\1 \2", s)
        
        s = str_operation_cleaning(s)
        
        s = re.sub(r"([0-9]+)( *)(inches|inch|in|')\.?", r"\1in. ", s)
        s = re.sub(r"([0-9]+)( *)(foot|feet|ft|'')\.?", r"\1ft. ", s)
        s = re.sub(r"([0-9]+)( *)(pounds|pound|lbs|lb)\.?", r"\1lb. ", s)
        s = re.sub(r"([0-9]+)( *)(square|sq) ?\.?(feet|foot|ft)\.?", r"\1sq.ft. ", s)
        s = re.sub(r"([0-9]+)( *)(gallons|gallon|gal)\.?", r"\1gal. ", s)
        s = re.sub(r"([0-9]+)( *)(ounces|ounce|oz)\.?", r"\1oz. ", s)
        s = re.sub(r"([0-9]+)( *)(centimeters|cm)\.?", r"\1cm. ", s)
        s = re.sub(r"([0-9]+)( *)(milimeters|mm)\.?", r"\1mm. ", s)
        s = re.sub(r"([0-9]+)( *)(degrees|degree)\.?", r"\1deg. ", s)
        s = re.sub(r"([0-9]+)( *)(volts|volt)\.?", r"\1volt. ", s)
        s = re.sub(r"([0-9]+)( *)(watts|watt)\.?", r"\1watt. ", s)
        s = re.sub(r"([0-9]+)( *)(amperes|ampere|amps|amp)\.?", r"\1amp. ", s)

        s = s.replace("  "," ")
        s = s.replace(" . "," ")
        s = s.replace("toliet","toilet")
        s = s.replace("airconditioner","air conditioner")
        s = s.replace("vinal","vinyl")
        s = s.replace("vynal","vinyl")
        s = s.replace("skill","skil")
        s = s.replace("snowbl","snow bl")
        s = s.replace("plexigla","plexi gla")
        s = s.replace("rustoleum","rust-oleum")
        s = s.replace("whirpool","whirlpool")
        s = s.replace("whirlpoolga", "whirlpool ga")
        s = s.replace("whirlpoolstainless","whirlpool stainless")
        # voir si c'est mieux sans stopwords, ou en enlevant differemment avec nltk
        s = (" ").join([z for z in s.split(" ") if z not in stop_w])
        s = (" ").join([str(strNum[z]) if z in strNum else z for z in s.split(" ")])
        s = (" ").join([stemmer.stem(z) for z in s.decode(encodingnumber).split(" ")])
        
        s = s.lower()
        return s
    else:
        return "null"

def str_common_word(str1, str2):
    words, cnt = str1.split(), 0
    for word in words:
        if str2.find(word)>=0:
            cnt+=1
    return cnt


#####################
## Distance metric ##
#####################

def cosine_sim(x, y):
    try:
        d = cosine_similarity(x, y)
        d = d[0][0]
    except:
        print x
        print y
        d = 0.
    return d

def JaccardCoef(A, B):
    A, B = set(A), set(B)
    intersect = len(A.intersection(B))
    union = len(A.union(B))
    coef = try_divide(intersect, union)
    return coef

def DiceDist(A, B):
    A, B = set(A), set(B)
    intersect = len(A.intersection(B))
    union = len(A) + len(B)
    d = try_divide(2*intersect, union)
    return d

def compute_dist(A, B, dist="jaccard_coef"):
    if dist == "jaccard_coef":
        d = JaccardCoef(A, B)
    elif dist == "dice_dist":
        d = DiceDist(A, B)
    return d

#### pairwise distance
def pairwise_jaccard_coef(A, B):
    coef = np.zeros((A.shape[0], B.shape[0]), dtype=float)
    for i in range(A.shape[0]):
        for j in range(B.shape[0]):
            coef[i,j] = JaccardCoef(A[i], B[j])
    return coef
    
def pairwise_dice_dist(A, B):
    d = np.zeros((A.shape[0], B.shape[0]), dtype=float)
    for i in range(A.shape[0]):
        for j in range(B.shape[0]):
            d[i,j] = DiceDist(A[i], B[j])
    return d

def pairwise_dist(A, B, dist="jaccard_coef"):
    if dist == "jaccard_coef":
        d = pairwise_jaccard_coef(A, B)
    elif dist == "dice_dist":
        d = pairwise_dice_dist(A, B)
    return d

####################################################################
####################################################################


###############
## READ DATA ##
###############

encodingnumber ="ISO-8859-1"
df_train = pd.read_csv('train.csv', encoding="ISO-8859-1")
df_test = pd.read_csv('test.csv', encoding="ISO-8859-1")
df_pro_desc = pd.read_csv('product_descriptions.csv')
df_attr = pd.read_csv('attributes.csv')

df_brand = df_attr[df_attr.name == "MFG Brand Name"][["product_uid", "value"]].rename(columns={"value": "brand"})

num_train = df_train.shape[0]
id_test = df_test['id']

df_all = pd.concat((df_train, df_test), axis=0, ignore_index=True)
df_all = pd.merge(df_all, df_pro_desc, how='left', on='product_uid')
df_all = pd.merge(df_all, df_brand, how='left', on='product_uid')
print("--- Files Loaded: %s minutes ---" % round(((time.time() - start_time)/60),2))

print "correction dict"

dico = get_dic()
for search in df_all['search_term'] :
    if search in dico.keys() :
        search = dico[search]

#######################
## SOME MORE METHODS ##
#######################

def get_position_list(target, obs):
    """
        Get the list of positions of obs in target
    """
    pos_of_obs_in_target = [0]
    if len(obs) != 0:
        pos_of_obs_in_target = [j for j,w in enumerate(obs, start=1) if w in target]
        if len(pos_of_obs_in_target) == 0:
            pos_of_obs_in_target = [0]
    return pos_of_obs_in_target

token_pattern = r"(?u)\b\w\w+\b"
#token_pattern = r'\w{1,}'
#token_pattern = r"\w+"
#token_pattern = r"[\w']+"

english_stemmer = nltk.stem.PorterStemmer()

def stem_tokens(tokens, stemmer):
    stemmed = []
    for token in tokens:
        stemmed.append(stemmer.stem(token))
    return stemmed


def preprocess_data(line,
                    token_pattern=token_pattern,
                    encode_digit=False):
    token_pattern = re.compile(token_pattern, flags = re.UNICODE | re.LOCALE)
    ## tokenize
    tokens = [x.lower() for x in token_pattern.findall(line)]
    ## stem
    tokens_stemmed = stem_tokens(tokens, english_stemmer)
    tokens_stemmed = [x for x in tokens_stemmed if x not in stop_w]
    return tokens_stemmed

def extract_feat(df):
    ## unigram
    print "generate unigram"
    df["query_unigram"] = list(df.apply(lambda x: preprocess_data(x["search_term"]), axis=1))
    df["title_unigram"] = list(df.apply(lambda x: preprocess_data(x["product_title"]), axis=1))
    df["description_unigram"] = list(df.apply(lambda x: preprocess_data(x["product_description"]), axis=1))
    ## bigram
    print "generate bigram"
    join_str = "_"
    df["query_bigram"] = list(df.apply(lambda x: ngramk.getBigram(x["query_unigram"], join_str), axis=1))
    df["title_bigram"] = list(df.apply(lambda x: ngramk.getBigram(x["title_unigram"], join_str), axis=1))
    df["description_bigram"] = list(df.apply(lambda x: ngramk.getBigram(x["description_unigram"], join_str), axis=1))
    ## trigram
    print "generate trigram"
    join_str = "_"
    df["query_trigram"] = list(df.apply(lambda x: ngramk.getTrigram(x["query_unigram"], join_str), axis=1))
    df["title_trigram"] = list(df.apply(lambda x: ngramk.getTrigram(x["title_unigram"], join_str), axis=1))
    df["description_trigram"] = list(df.apply(lambda x: ngramk.getTrigram(x["description_unigram"], join_str), axis=1))


    ################################
    ## word count and digit count ##
    ################################
    print "generate word counting features"
    feat_names = ["query", "title", "description"]
    grams = ["unigram", "bigram", "trigram"]
    count_digit = lambda x: sum([1. for w in x if w.isdigit()])
    for feat_name in feat_names:
        for gram in grams:
            ## word count
            df["count_of_%s_%s"%(feat_name,gram)] = list(df.apply(lambda x: len(x[feat_name+"_"+gram]), axis=1))
            df["count_of_unique_%s_%s"%(feat_name,gram)] = list(df.apply(lambda x: len(set(x[feat_name+"_"+gram])), axis=1))
            df["ratio_of_unique_%s_%s"%(feat_name,gram)] = map(try_divide, df["count_of_unique_%s_%s"%(feat_name,gram)], df["count_of_%s_%s"%(feat_name,gram)])

        ## digit count
        df["count_of_digit_in_%s"%feat_name] = list(df.apply(lambda x: count_digit(x[feat_name+"_unigram"]), axis=1))
        df["ratio_of_digit_in_%s"%feat_name] = map(try_divide, df["count_of_digit_in_%s"%feat_name], df["count_of_%s_unigram"%(feat_name)])



    ##########################
    ## intersect word count ##
    ##########################
    print "generate intersect word counting features"
    #### unigram
    for gram in grams:
        for obs_name in feat_names:
            for target_name in feat_names:
                if target_name != obs_name:
                    ## query
                    df["count_of_%s_%s_in_%s"%(obs_name,gram,target_name)] = list(df.apply(lambda x: sum([1. for w in x[obs_name+"_"+gram] if w in set(x[target_name+"_"+gram])]), axis=1))
                    df["ratio_of_%s_%s_in_%s"%(obs_name,gram,target_name)] = map(try_divide, df["count_of_%s_%s_in_%s"%(obs_name,gram,target_name)], df["count_of_%s_%s"%(obs_name,gram)])

        ## some other feat
        df["title_%s_in_query_div_query_%s"%(gram,gram)] = map(try_divide, df["count_of_title_%s_in_query"%gram], df["count_of_query_%s"%gram])
        df["title_%s_in_query_div_query_%s_in_title"%(gram,gram)] = map(try_divide, df["count_of_title_%s_in_query"%gram], df["count_of_query_%s_in_title"%gram])
        df["description_%s_in_query_div_query_%s"%(gram,gram)] = map(try_divide, df["count_of_description_%s_in_query"%gram], df["count_of_query_%s"%gram])
        df["description_%s_in_query_div_query_%s_in_description"%(gram,gram)] = map(try_divide, df["count_of_description_%s_in_query"%gram], df["count_of_query_%s_in_description"%gram])


    ##################################
    ## intersect word position feat ##
    ##################################
    print "generate intersect word position features"
    for gram in grams:
        for target_name in feat_names:
            for obs_name in feat_names:
                if target_name != obs_name:
                    pos = list(df.apply(lambda x: get_position_list(x[target_name+"_"+gram], obs=x[obs_name+"_"+gram]), axis=1))
                    ## stats feat on pos
                    df["pos_of_%s_%s_in_%s_min" % (obs_name, gram, target_name)] = map(np.min, pos)
                    df["pos_of_%s_%s_in_%s_mean" % (obs_name, gram, target_name)] = map(np.mean, pos)
                    df["pos_of_%s_%s_in_%s_median" % (obs_name, gram, target_name)] = map(np.median, pos)
                    df["pos_of_%s_%s_in_%s_max" % (obs_name, gram, target_name)] = map(np.max, pos)
                    df["pos_of_%s_%s_in_%s_std" % (obs_name, gram, target_name)] = map(np.std, pos)
                    ## stats feat on normalized_pos
                    df["normalized_pos_of_%s_%s_in_%s_min" % (obs_name, gram, target_name)] = map(try_divide, df["pos_of_%s_%s_in_%s_min" % (obs_name, gram, target_name)], df["count_of_%s_%s" % (obs_name, gram)])
                    df["normalized_pos_of_%s_%s_in_%s_mean" % (obs_name, gram, target_name)] = map(try_divide, df["pos_of_%s_%s_in_%s_mean" % (obs_name, gram, target_name)], df["count_of_%s_%s" % (obs_name, gram)])
                    df["normalized_pos_of_%s_%s_in_%s_median" % (obs_name, gram, target_name)] = map(try_divide, df["pos_of_%s_%s_in_%s_median" % (obs_name, gram, target_name)], df["count_of_%s_%s" % (obs_name, gram)])
                    df["normalized_pos_of_%s_%s_in_%s_max" % (obs_name, gram, target_name)] = map(try_divide, df["pos_of_%s_%s_in_%s_max" % (obs_name, gram, target_name)], df["count_of_%s_%s" % (obs_name, gram)])
                    df["normalized_pos_of_%s_%s_in_%s_std" % (obs_name, gram, target_name)] = map(try_divide, df["pos_of_%s_%s_in_%s_std" % (obs_name, gram, target_name)] , df["count_of_%s_%s" % (obs_name, gram)])


    ## jaccard coef/dice dist of n-gram
    print "generate jaccard coef and dice dist for n-gram"
    dists = ["jaccard_coef", "dice_dist"]
    grams = ["unigram", "bigram", "trigram"]
    feat_names = ["query", "title", "description"]
    for dist in dists:
        for gram in grams:
            for i in range(len(feat_names)-1):
                for j in range(i+1,len(feat_names)):
                    target_name = feat_names[i]
                    obs_name = feat_names[j]
                    df["%s_of_%s_between_%s_%s"%(dist,gram,target_name,obs_name)] = \
                            list(df.apply(lambda x: compute_dist(x[target_name+"_"+gram], x[obs_name+"_"+gram], dist), axis=1))


#############################################################
########### USE METHODS TO CREATE NEW FEATURES ##############
#############################################################



extract_feat(df_all)
feat_names = [ name for name in df_all.columns \
            if "count" in name \
            or "ratio" in name \
            or "div" in name \
            or "pos_of" in name
    ]

data_all = df_all[feat_names[0]]
for feat_name in feat_names[1:] :
    data_all = pd.concat((data_all, df_all[feat_name]), axis = 1)


###############################
########## CLEAN DATA #########
###############################

print "1"
df_all['search_term'] = df_all['search_term'].map(lambda x:str_stem(x.encode(encodingnumber)))
print "1.25"
df_all['product_title'] = df_all['product_title'].map(lambda x:str_stem(x.encode(encodingnumber)))
print "1.5"
df_all['product_description'] = df_all['product_description'].map(lambda x:str_stem(x.encode(encodingnumber)))
print "1.75"
df_all['brand'] = df_all['brand'].map(lambda x:str_stem(x))
print "soon"
df_all['product_info'] = df_all['search_term']+"\t"+df_all['product_title'] +"\t"+df_all['product_description']


###########################################
######### CREATE SOME MORE FEATURES #######
###########################################


print "2"
df_all['len_of_query'] = df_all['search_term'].map(lambda x:len(x.split())).astype(np.int64)
df_all['len_of_title'] = df_all['product_title'].map(lambda x:len(x.split())).astype(np.int64)
df_all['len_of_description'] = df_all['product_description'].map(lambda x:len(x.split())).astype(np.int64)
df_all['len_of_brand'] = df_all['brand'].map(lambda x:len(x.split())).astype(np.int64)

print "3"
df_all['word_in_title'] = df_all['product_info'].map(lambda x:str_common_word(x.split('\t')[0],x.split('\t')[1]))
df_all['word_in_description'] = df_all['product_info'].map(lambda x:str_common_word(x.split('\t')[0],x.split('\t')[2]))
df_all['titlehits'] = df_all[['search_term','product_title']].apply(lambda x: str_common_word(x['search_term'],x['product_title']),axis=1)
df_all['query_in_title'] = df_all['product_info'].map(lambda x:str_whole_word(x.split('\t')[0],x.split('\t')[1],0))
df_all['query_in_description'] = df_all['product_info'].map(lambda x:str_whole_word(x.split('\t')[0],x.split('\t')[2],0))
df_all['query_last_word_in_title'] = df_all['product_info'].map(lambda x:str_common_word(x.split('\t')[0].split(" ")[-1],x.split('\t')[1]))
df_all['quotuery_last_word_in_description'] = df_all['product_info'].map(lambda x:str_common_word(x.split('\t')[0].split(" ")[-1],x.split('\t')[2]))

print "4"
df_all['ratio_title'] = df_all['word_in_title']/df_all['len_of_query']
df_all['ratio_description'] = df_all['word_in_description']/df_all['len_of_query']
df_all['attr'] = df_all['search_term']+"\t"+df_all['brand']
df_all['word_in_brand'] = df_all['attr'].map(lambda x:str_common_word(x.split('\t')[0],x.split('\t')[1]))
df_all['ratio_brand'] = df_all['word_in_brand']/df_all['len_of_brand']
df_brand = pd.unique(df_all.brand.ravel())


df_all['search_term_feature'] = df_all['search_term'].map(lambda x:len(x))

print "5"
print "6"
df_all.to_csv('df_all_PREPRO_GOOD.csv', encoding="utf-8")


####################################
####### ONE LAST FUNCTION ##########
####################################

def get_tf_idf_df(dataframe, dimension, range):
    # construit une tf_idf_matrix a partir dun texte donnee et reduit la dimension avec truncated SVD
    tf = TfidfVectorizer(ngram_range=(1,range), stop_words = 'english')
    tfidf_matrix =  tf.fit_transform(dataframe)
    # tfidf_matrix = tfidf_matrix.todense()

    svd = TruncatedSVD(n_components = dimension, random_state=42, n_iter = 30)
    tfidf_reduc = svd.fit_transform(tfidf_matrix)
    print "variance ratio"+str(dataframe.name), svd.explained_variance_ratio_.sum()
    res = pd.DataFrame(tfidf_reduc)
    res.rename(columns = lambda x: str(x)+str(dataframe.name)+str(range), inplace = True)
    return res

dimereduc0 = 25
dimereduc1 = 150
dimereduc2 = 200
# to_drop = ['id','search_term','product_title','product_description','product_info','attr','brand']
to_get = [

'len_of_query', 'len_of_title' ,'len_of_description' ,'len_of_brand', 'word_in_title' ,'word_in_description' ,'titlehits', 'query_in_title'
        ,'query_in_description'
,'query_last_word_in_title'
,'quotuery_last_word_in_description'
,'ratio_title','ratio_description','word_in_brand'
,'ratio_brand','search_term_feature',
"relevance"]

nrange1 = 1
nrange2 = 2
numslice = df_all["search_term"].shape[0]

print "make tf-idf"
tfidf_total = get_tf_idf_df(pd.concat((df_all['search_term'], df_all['product_title'], df_all['product_description'], df_all['brand']), axis = 0) ,dimereduc0, nrange1)

tfidf_search_n1 =  tfidf_total[:numslice].reset_index()
tfidf_title_n1 = tfidf_total[numslice:numslice*2].reset_index()
tfidf_description_n1 = tfidf_total[numslice*2:numslice*3].reset_index()
tfidf_brand_n1 = tfidf_total[numslice*3:].reset_index()

# tfidf_product_title_n1 = get_tf_idf_df(df_all['product_title'],dimereduc0,nrange1)
# tfidf_product_description_n1 =   get_tf_idf_df(df_all['product_description'],dimereduc0,nrange1)
# tfidf_brand_n1 =  get_tf_idf_df(df_all['brand'],dimereduc0,nrange1)

# df_tfidf1 = pd.concat((tfidf_search_n1, tfidf_product_description_n1, tfidf_product_description_n1, tfidf_brand_n1), axis = 1)

df_tfidf1 = pd.concat((tfidf_search_n1, tfidf_title_n1, tfidf_description_n1, tfidf_brand_n1), axis = 1)

##############################
###### GET COSINE SIM ########
##############################

print "get cosine sim"
print "building idf unigram & bigram for cosine sim"

#### GET UNIGRAM AND BIGRAM IDF #######

query_title = pd.concat((df_all["search_term"],df_all["product_title"]), axis = 0)
query_title_idf1 = get_tf_idf_df(query_title,dimereduc1,nrange1)
query_title_idf2 = get_tf_idf_df(query_title,dimereduc1,nrange2)

query_description = pd.concat((df_all["search_term"],df_all["product_description"]), axis = 0)
query_description_idf1 = get_tf_idf_df(query_description,dimereduc1,nrange1)
query_description_idf2 = get_tf_idf_df(query_description,dimereduc2,nrange2)

query_brand = pd.concat((df_all["search_term"],df_all["brand"]), axis = 0)
query_brand_idf1 = get_tf_idf_df(query_brand,dimereduc1, nrange1)
query_brand_idf2 = get_tf_idf_df(query_brand,dimereduc2, nrange2)


###### SLICE FOR UNIGRAM ########

tfQT_query1 = query_title_idf1[:numslice]
tfQT_title1 = query_title_idf1[numslice:]
tfQD_query1 = query_description_idf1[:numslice]
tfQD_description1 = query_description_idf1[numslice:]
tfQB_query1 =  query_brand_idf1[:numslice]
tfQB_brand1 = query_brand_idf1[numslice:]

####  SLICE FOR BIGRAM ######

tfQT_query2 = query_title_idf2[:numslice]
tfQT_title2 = query_title_idf2[numslice:]
tfQD_query2 = query_description_idf2[:numslice]
tfQD_description2 = query_description_idf2[numslice:]
tfQB_query2 = query_brand_idf2[numslice:]
tfQB_brand2 = query_brand_idf2[:numslice]

#### GET COSINE DIST ####

print "comuting cosine dist"
cosine_dist_QT1 = [cosine_sim(tfQT_query1.iloc[[i]],tfQT_title1.iloc[[i]]) for i in range(0,numslice)]
cosine_dist_QD1 = [cosine_sim(tfQD_query1.iloc[[i]],tfQD_description1.iloc[[i]]) for i in range (0,numslice)]
cosine_dist_QB1 = [cosine_sim(tfQB_query1.iloc[[i]],tfQB_brand1.iloc[[i]]) for i in range (0,numslice)]
cosine_dist_QT2 = [cosine_sim(tfQT_query2.iloc[[i]],tfQT_title2.iloc[[i]]) for i in range(0,numslice)]
cosine_dist_QD2 = [cosine_sim(tfQD_query2.iloc[[i]],tfQD_description2.iloc[[i]]) for i in range (0,numslice)]
cosine_dist_QB2 = [cosine_sim(tfQB_query2.iloc[[i]],tfQB_brand2.iloc[[i]]) for i in range (0,numslice)]

pd1 = pd.DataFrame(cosine_dist_QT1, columns = ["cosQT1"])
pd2 = pd.DataFrame(cosine_dist_QD1, columns = ["cosQD1"])
pd3 = pd.DataFrame(cosine_dist_QB1, columns = ["cosQB1"])
pd4 = pd.DataFrame(cosine_dist_QT2, columns = ["cosQT2"])
pd5 = pd.DataFrame(cosine_dist_QD2, columns = ["cosQD2"])
pd6 = pd.DataFrame(cosine_dist_QB2, columns = ["cosQB2"])

df_cosine_dist = pd.concat((pd1,pd2,pd3,pd4,pd5,pd6), axis = 1)


#### CONCAT COUNTING FEATURES / DISTANCES / COSINE SIM ####

df_all_ok = df_all[to_get]
df_all_ok = pd.concat((df_all_ok,df_cosine_dist), axis =1)


#####################################################
### Compute gow and make dimensionality reduction ###
#####################################################

col_names =  ['product_title', 'search_term', 'product_description' ]
dimensions = [90, 120, 150]

alpha_PR = 0.99
max_iter_ = 900
tolerance = 1e-05


### set window size
window_sizes = [4,4,4]
###############
# do the same for other col_names : brand ? or use it as categorical data ?
df_gow = pd.DataFrame()
for i  in range(len(col_names)) :
    col_name = col_names[i]
    dim_reduc = dimensions[i]
    window_size = window_sizes[i]
    print "Taking care of " + str(col_name)  
    product_PR = compute_gow(df_all, col_name, wind_size = window_size)
    print "PageRank results size :"
    print getsizeof(product_PR)
    print "Dict Vectorizing..."
    v = DictVectorizer(sparse = True)
    print getsizeof(v)
    to_matrix = v.fit_transform(df_all['gow_'+col_name].tolist())
    print type(to_matrix)
    print getsizeof(to_matrix)
    print to_matrix.shape
    print "Done!\n"
    svd = TruncatedSVD(n_components=dim_reduc, random_state=42, n_iter = 15)
    reduced_matrix = svd.fit_transform(to_matrix)
    #ut, s, vt = sparsesvd(csc_matrix(to_matrix), dim_reduc)
    #reduced_matrix = np.transpose(ut)
    
    print reduced_matrix.shape
    df_aux = pd.DataFrame(reduced_matrix, columns = [col_name+"_gowfeat_"+str(i) for i in xrange(reduced_matrix.shape[1])])
    df_gow = pd.concat([df_gow, df_aux], axis = 1)
    print df_all.shape
    print "For %s reducing dimension to %d captures %f percent of variance."%(col_name, dim_reduc, np.sum(svd.explained_variance_ratio_)*100)

    print "Deleting + garbage collector...\n"
    del df_aux
    del v
    del to_matrix
    gc.collect()
print "Done!\n"



print "saving data"

#### Get labels ####
Y_train = df_all_ok['relevance'].values
del df_all_ok["relevance"]
Y_train = Y_train[:num_train]

#### CONCAT WITH GOW AND TF-IDF FEATURES ####
tout_traite = pd.concat((df_all_ok, data_all,df_tfidf1, df_gow), axis = 1)

##### Recreate train - test ####
df_train  = tout_traite[:num_train]
df_test = tout_traite[num_train:]

#### Taking the values ####
X_train = df_train
X_test = df_test


#######################################
############# SAVE DATA ###############
#######################################

X_train_save_df = pd.DataFrame(X_train).to_csv('X_train_countJ_gow_final.csv', index = False)
Y_train_save_df = pd.DataFrame(Y_train).to_csv('Y_train_countJ_gow_final.csv', index = False)
X_test_save_df = pd.DataFrame(X_test).to_csv('X_test_countJ_gow_final.csv', index = False)

print "Done ! Ready for train & opti"
