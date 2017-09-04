import configparser
import sys
import time

from features.enrichWithFeatures import *


def main(config):
    
    bin_ranges = getlist(config['RETURN_BINS'].get("bin_ranges"), float)
    bin_names = getlist(config['RETURN_BINS'].get("bin_names"), str)
    
    print("Loading feature file.!")
    # old inputData features
    try:
        features = pd.read_csv("data/"+sys.argv[2])
    except:
        print("File does not exist : "+sys.argv[2])
        sys.exit(0)
    features.index = features.index + 1    
    
    print("Loading raw data file.!")
    # raw inputData data
    try:
        data = pd.read_csv("data/"+sys.argv[1])
    except:
        print("File does not exist : "+sys.argv[1])
        sys.exit(0)
    data.index = data.index + 1
    
    # data to be passed to functions ,Picking 9 records.
    index = data[data['Date'] == sys.argv[4]].index.values
    if(len(index) == 0):
        print("Key not Found. Check the date properly.")
        sys.exit(0)
    
    #data is not present if index is zero
    start = features.last_valid_index()
    end = index[0]
    n = end - start
    if(n == 0):
        print("Nothing To Update")
        sys.exit(0)
    interim = data[start-9:end]
    
    # Functions for features calculations
    print("Feature calculation Started.!")
    
    f = Features(features, n, bin_ranges, bin_names, config)
    featureList = []
    if(sys.argv[5] == "ess"):
        featureList = [ 
                       data.ix[start+1:end],  
                       f.closeByFeatures(interim, "High"),
                       f.closeByFeatures(interim, "Low"),
                       f.getCCFeatures(interim, 3),
                       f.getCCFeatures(interim, 5), 
                       f.getStcFeatures(interim, 7),
                       f.getStcFeatures(interim, 3), 
                       f.getRSISmoothedFeatures(interim, 3), 
                       f.getMcdFeatures(interim, 5, 3, 1),
                       f.getMcdFeatures(interim, 5, 3, 2), 
                       f.typeDFeatures(interim, 1),
                       f.typeDFeatures(interim, 2),
                       f.typeDFeatures(interim, 3), 
                       f.typeDFeatures(interim, 5)
                       ]
    elif(sys.argv[5] == "tny"):
        featureList = [ 
                       data.ix[start+1:end],  
                       f.typeDFeatures(interim, 1),
                       f.typeDFeatures(interim, 2),
                       f.getCCFeatures(interim, 3),
                       f.getCCFeatures(interim, 5), 
                       f.getCCFeatures(interim, 9),
                       f.getMomentumFeatures(interim,3),
                       f.getMomentumFeatures(interim,5),
                       f.getMomentumFeatures(interim,7),
                       f.getMomentumFeatures(interim,9),
                       f.getStcFeaturesTny(interim, 3),
                       f.getStcFeaturesTny(interim, 5),
                       f.getStcFeaturesTny(interim, 7),
                       f.getRSIRegularFeatures(interim, 3),
                       f.getRSIRegularFeatures(interim, 5),
                       f.getRSIRegularFeatures(interim, 7),
                       f.getMcdFeaturesTny(interim, 5, 3),
                       f.getMcdFeaturesTny(interim, 7, 3),
                       f.getMcdFeaturesTny(interim, 7, 5),
                       f.getMcdFeaturesTny(interim, 9, 3)
                       ] 
    else:
        print("Unknown format.Please provide ess or tny")
        sys.exit(0)
    print("Feature calculation Finished.!")
     
    final = pd.concat(featureList, axis=1)
    final = final.loc[:, ~final.columns.duplicated()]
    
    global cols
    # Columns format used to write output file
    if(sys.argv[5] == "ess"):
        cols = ['ID', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close', 'Clo_by_High_min95',
                'Close_by_Low_min95','TPV', 'SMA_TPV(3)', 'MAD(3)', 'CCI_3', 'SMA_TPV(5)', 'MAD(5)', 
                'CCI_5', 'CCI_5_SIGNAL', 'Slow(5)', 'Fast(3)', 'macd_5_3_1', 'SigLine(2)', 'macd_5_3_2',
                'UP', 'DOWN', 'UP_AVG_3', 'DOWN_AVG_3', 'Relative_S_3', 'RSI_3', 'RSI_3_Category', 'STOC_3_LOW',
                'STOC_3_MIN_MAX', 'STOC_3_K', 'STOC_3_K_CAT', 'STOC_7_LOW', 'STOC_7_MIN_MAX',
                'STOC_7_K', 'STOC_7_percD', 'STOC_7_K-D', '1D_r', '1D_r_cat', '2D_r', '2D_r_cat',
                '3D_r', '3D_r_cat', '5D_r', '5D_r_cat']
    elif(sys.argv[5] == "tny"):
        cols = ['ID','Date','Open','High','Low','Close','TPV','SMA_TPV(3)','MAD(3)','CCI_3',
                'SMA_TPV(5)','MAD(5)','CCI_5','SMA_TPV(9)', 'MAD(9)', 'CCI_9','momentum_3',
                'momentum_5','momentum_7','momentum_9','STOC_3_LOW','STOC_RANGE_3', 'sto_3',
                'STOC_5_LOW','STOC_RANGE_5', 'sto_5','STOC_7_LOW','STOC_RANGE_7','sto_7',
                'UP','DOWN','UP_AVG_3','DOWN_AVG_3','Relative_S_3','rsi_3','UP_AVG_5',
                'DOWN_AVG_5','Relative_S_5','rsi_5','UP_AVG_7','DOWN_AVG_7','Relative_S_7',
                'rsi_7','Slow(9)','Slow(7)','Slow(5)','Fast(3)','macd_5_3','macd_7_3','macd_7_5',
                'macd_9_3','1D_r','2D_r']

    print("Writing Output File.!")
    features[cols].append(final[cols], ignore_index=True).to_csv("data/"+sys.argv[3], index=False)


def getlist(option, dataType):
    """Return a list from a ConfigParser option"""
    return [ dataType(chunk) for chunk in option.split(",") ]

if __name__ == "__main__":
    
    start_time = time.time()
    
    if (len(sys.argv) < 6):
        print("""\
This script will produce the features for stock prediction.

Usage: python run_all.py inputDatacsvfile inputDatafeaturefile outputfilename date (ess or tny)
help : Date format - MM/DD/YYYY
            """)
        sys.exit(0)

    print("Started.!")  
    config = configparser.ConfigParser()
    config.read('utils/config.ini')
    
        
    main(config)
    print("Feature Generation Complete.!")
    print("--- %s seconds for execution ---" % (time.time() - start_time))
