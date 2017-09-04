#######################################################################################
# Filename         : enrichWithFeatures.py                                            #
# Author             :                                                                #
# Date             : 30/January/2017                                                  #
# Description      : Purpose of this code is to generate the various stock indicators.#
#                    The indicators are further used by ML algorithms for prediction. #                 
# Input parameters : @Raw_inputData File,                                             #
#                    @Old Feature File,                                               #
#                    @Today's Date,                                                   #
#                    @Output file,                                                    #
#                    @number of output records(optional,default =1)                   #
# Output           : New file with features for today's record.                       #
#######################################################################################

import numpy as np
import pandas as pd


class Features:
    def __init__(self, features, n, bin_ranges, group_names, config):
        self.features = features
        self.n = n
        self.bin_ranges = bin_ranges
        self.group_names = group_names
        self.config = config
        
    def getMcdFeatures(self, inputData, type1, type2, type3):
        
        mcd = pd.DataFrame()
        iteration = inputData['ID'].values
        
        tempFeatures = self.features
        for i in iteration[-self.n:]:
           
            #Slow = (Close-Slow(-1))*0.333+Slow(-1)
            slow = (inputData.ix[i]['Close'] - tempFeatures.ix[i - 1]['Slow(' + str(type1) + ')']) * 0.333 + tempFeatures.ix[i - 1]['Slow(' + str(type1) + ')']
            #Fast = (Close-Fast(-1))*0.5+Fast(-1)
            fast = (inputData.ix[i]['Close'] - tempFeatures.ix[i - 1]['Fast(' + str(type2) + ')']) * 0.5 + tempFeatures.ix[i - 1]['Fast(' + str(type2) + ')']
            mcd.set_value(i, 'Slow(' + str(type1) + ')', slow)
            mcd.set_value(i, 'Fast(' + str(type2) + ')', fast)
            
            #macd = Fast - Slow
            mcd_type = fast - slow
            mcd.set_value(i, 'macd_' + str(type1) + '_' + str(type2) + '_' + str(type3), mcd_type)
            
            if(type3 == 2):
                #signal = (macd - signal(-1))*0.67+signal(-1)
                signal = (mcd_type - tempFeatures.ix[i - 1]['SigLine(' + str(type3) + ')']) * 0.67 + tempFeatures.ix[i - 1]['SigLine(' + str(type3) + ')']
                mcd.set_value(i, 'SigLine(' + str(type3) + ')', signal)
                mcd.set_value(i, 'macd_' + str(type1) + '_' + str(type2) + '_' + str(type3), mcd_type - signal)
            
            tempFeatures = pd.concat([tempFeatures,mcd])
            
        return mcd
    
    
    def getMcdFeaturesTny(self, inputData, indicator1 , indicator2):
        mcd = pd.DataFrame()
        iteration = inputData['ID'].values
        
        for i in iteration[-self.n:]:
            slow = inputData.ix[i-indicator1+1:i]['Close'].sum()/indicator1
            
            if(indicator2 == 3):
                global fast
                fast = inputData.ix[i-indicator2+1:i]['Close'].sum()/indicator2
                mcd.set_value(i, 'Fast(' + str(indicator2) + ')', fast)
            else:
                global slow1
                slow1 = inputData.ix[i-indicator2+1:i]['Close'].sum()/indicator2
                mcd.set_value(i, 'Slow(' + str(indicator2) + ')', slow1)
                
            mcd.set_value(i, 'Slow(' + str(indicator1) + ')', slow)
            
            if(indicator2 == 3):
                mcd_type = (fast - slow)*100
                mcd.set_value(i, 'macd_' + str(indicator1) + '_' + str(indicator2), mcd_type)
            else:
                mcd_type = (slow1 - slow)*100
                mcd.set_value(i, 'macd_' + str(indicator1) + '_' + str(indicator2), mcd_type)
            
        return mcd
    
    
    def typeDFeatures(self, inputData, typeIndicator):
        typed = pd.DataFrame()
        iteration = inputData['ID'].values
        
        for i in iteration[-self.n:]:
            typed_r = ((inputData.ix[i]['Close'] - inputData.ix[i - typeIndicator]['Close']) / inputData.ix[i - typeIndicator]['Close']) * 100
            typed.set_value(i, str(typeIndicator) + 'D_r', typed_r)
        typed[str(typeIndicator) + 'D_r_cat'] = pd.cut(typed[str(typeIndicator) + 'D_r'].values, self.bin_ranges, labels=self.group_names)
       
        return typed
        
    
    def getStcFeatures(self, inputData, typeIndicator):
        stc = pd.DataFrame()
        iteration = inputData['ID'].values
        tempFeatures = self.features
        for i in iteration[-self.n:]:
            stoc_low = inputData.ix[i]['Close'] - min(inputData.ix[i - typeIndicator + 1:i]['Low'])
            stc.set_value(i, 'STOC_' + str(typeIndicator) + '_LOW', stoc_low)
            
            stoc_min_max = max(inputData.ix[i - typeIndicator + 1:i]['High']) - min(inputData.ix[i - typeIndicator + 1:i]['Low'])
            stc.set_value(i, 'STOC_' + str(typeIndicator) + '_MIN_MAX', stoc_min_max)
            
            stoc_k = (stoc_low / stoc_min_max) * 100
            stc.set_value(i, 'STOC_' + str(typeIndicator) + '_K', stoc_k)
            
            stoc_percD = (stoc_k + tempFeatures[-typeIndicator+1:]['STOC_' + str(typeIndicator) + '_K'].sum()) / typeIndicator 
            stc.set_value(i, 'STOC_' + str(typeIndicator) + '_percD', stoc_percD)
    
            stc.set_value(i, 'STOC_' + str(typeIndicator) + '_K-D', stoc_k - stoc_percD)

            tempFeatures = tempFeatures.append(stc,ignore_index=True)
        bin_names = self.getlist(self.config['STOC_BINS'].get("bin_names"), str)
        bin_ranges = self.getlist(self.config['STOC_BINS'].get("bin_ranges"), float)
        stc['STOC_' + str(typeIndicator) + '_K_CAT'] = pd.cut(stc['STOC_' + str(typeIndicator) + '_K'].values, bin_ranges, labels=bin_names, include_lowest=True)
            
        return stc
    
    def getStcFeaturesTny(self, inputData, typeIndicator):
       
        stc = pd.DataFrame()
        iteration = inputData['ID'].values
        tempFeatures = self.features
        for i in iteration[-self.n:]:
            stoc_low = inputData.ix[i]['Close'] - min(inputData.ix[i - typeIndicator + 1:i]['Close'])
            stc.set_value(i, 'STOC_' + str(typeIndicator) + '_LOW', stoc_low)
            
            stoc_min_max = max(inputData.ix[i - typeIndicator + 1:i]['Close']) - min(inputData.ix[i - typeIndicator + 1:i]['Close'])
            stc.set_value(i, 'STOC_' + 'RANGE_' +str(typeIndicator) , stoc_min_max)
            
            stoc_k = (stoc_low / stoc_min_max) * 100
            stc.set_value(i, 'sto_' + str(typeIndicator), stoc_k)

            tempFeatures = tempFeatures.append(stc,ignore_index=True)
            
        return stc
    
    
    def getRSISmoothedFeatures(self, inputData, strength):
        '''
            TODO-COMMENT
        '''
        
        up = self.features.loc[self.features.last_valid_index()]['UP_AVG_' + str(strength)]
        down = self.features.loc[self.features.last_valid_index()]['DOWN_AVG_' + str(strength)]
        rs = 0.
        rsi = pd.DataFrame()
        
        dataClose = inputData['Close']
        iteration = inputData['ID'].values
        
        for i in iteration[-self.n:]:
            delta = dataClose[i] - dataClose[i - 1]
            
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = abs(delta)
            
            up = (up * (strength - 1) + upval) / strength
            down = (down * (strength - 1) + downval) / strength
            
            rsi.set_value(i, 'UP', upval)
            rsi.set_value(i, 'DOWN', downval)
            rsi.set_value(i, 'UP_AVG_' + str(strength), up)
            rsi.set_value(i, 'DOWN_AVG_' + str(strength), down)
            
            rs = up / down
            rsi.set_value(i, 'Relative_S_' + str(strength), rs)
            rsi_perc = 100. - 100. / (1. + rs)
            rsi.set_value(i, 'RSI_' + str(strength), rsi_perc)
            
        bin_names = self.getlist(self.config['RSI_BINS'].get("bin_names"), str)
        bin_ranges = self.getlist(self.config['RSI_BINS'].get("bin_ranges"), float)
        rsi['RSI_' + str(strength) + '_Category'] = pd.cut(rsi['RSI_' + str(strength)].values, bin_ranges, labels=bin_names, include_lowest=True)
               
        return rsi
    
    
    def getRSIRegularFeatures(self, inputData, strength):
        rs = 0.
        rsi = pd.DataFrame()
        tempFeatures = self.features
        
        dataClose = inputData['Close']
        iteration = inputData['ID'].values
        
        for i in iteration[-self.n:]:
            delta = dataClose[i] - dataClose[i - 1]
            
            if delta > 0:
                upval = delta * 100
                downval = 0.
            else:
                upval = 0.
                downval = abs(delta) * 100
            
            up = (upval + tempFeatures[-strength+1:]['UP'].sum()) / strength 
            down = (downval + tempFeatures[-strength+1:]['DOWN'].sum()) / strength
            
            rsi.set_value(i, 'UP', upval)
            rsi.set_value(i, 'DOWN', downval)
            rsi.set_value(i, 'UP_AVG_' + str(strength), up)
            rsi.set_value(i, 'DOWN_AVG_' + str(strength), down)
            
            rs = up / down
            rsi.set_value(i, 'Relative_S_' + str(strength), rs)
            rsi_perc = 100. - 100. / (1. + rs)
            rsi.set_value(i, 'rsi_' + str(strength), rsi_perc)
            
            tempFeatures = tempFeatures.append(rsi,ignore_index=True)
            
        return rsi
    
    
    def getCCFeatures(self, inputData, typeIndicator):
        cci = pd.DataFrame()
        iteration = inputData['ID'].values
        tempFeatures = self.features
        for i in iteration[-self.n:]:
            #(high+low+close)/3
            tpv = (inputData.ix[i]['High'] + inputData.ix[i]['Low'] + inputData.ix[i]['Close']) / 3
            cci.set_value(i, 'TPV', tpv)
            
            #Average of tpv based on strength . Example : If 3, then last 3 values average.
            sma_tpv = (tpv + tempFeatures[-typeIndicator+1:]['TPV'].sum()) / typeIndicator
            cci.set_value(i, 'SMA_TPV(' + str(typeIndicator) + ')', sma_tpv)
            
            #Mean absoulte deviation of tpv,sma_tpv
            mad_type = np.mean(np.absolute(tempFeatures[-typeIndicator+1:]['TPV'].append(pd.Series([tpv])) - sma_tpv))
            cci.set_value(i, 'MAD(' + str(typeIndicator) + ')', mad_type)
            
            cci_type = ((tpv - sma_tpv) / (0.015 * mad_type))
            cci.set_value(i, 'CCI_' + str(typeIndicator), cci_type)
            
            if(cci_type < -150):
                cci.set_value(i, 'CCI_' + str(typeIndicator) + '_SIGNAL', "POS")
            elif(cci_type < 0):
                cci.set_value(i, 'CCI_' + str(typeIndicator) + '_SIGNAL', "NEG")
            elif(cci_type < 150):
                cci.set_value(i, 'CCI_' + str(typeIndicator) + '_SIGNAL', "POS")
            else:
                cci.set_value(i, 'CCI_' + str(typeIndicator) + '_SIGNAL', "NEG")
            tempFeatures = tempFeatures.append(cci,ignore_index=True)
            
        return cci
    
    
    def getMomentumFeatures(self, inputData, typeIndicator):
        mom = pd.DataFrame()
        
        dataClose = inputData['Close']
        iteration = inputData['ID'].values
        for i in iteration[-self.n:]:
            delta = ((dataClose[i] - dataClose[i - typeIndicator + 1]) / dataClose[i - typeIndicator + 1]) * 100
            mom.set_value(i, "momentum_" + str(typeIndicator), delta)
           
        return mom
    
    
    def closeByFeatures(self, inputData, typeIndicator):
        clsByFeature = pd.DataFrame()
        
        iteration = inputData['ID'].values
        for i in iteration[-self.n:]:
            if(typeIndicator == "High"):
                delta = ((inputData['Close'][i] / inputData['High'][i]) - 0.95) * 100
                clsByFeature.set_value(i, "Clo_by_"+  str(typeIndicator) +"_min95", delta)
            elif(typeIndicator == "Low"):
                delta = ((inputData['Close'][i] / inputData['Low'][i]) - 0.95) * 100
                clsByFeature.set_value(i, "Close_by_"+  str(typeIndicator) +"_min95", delta)
         
        return clsByFeature
    
    
    def getlist(self, option, dataType):
        """Return a list from a ConfigParser option"""
        return [ dataType(chunk) for chunk in option.split(",") ]

