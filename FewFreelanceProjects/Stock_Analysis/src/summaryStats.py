import sys
import time
import pandas as pd


def statsCalculate(data, indicator):
    data['% Gain/Loss'] = data['% Gain/Loss'].replace('%','',regex=True).astype('float')
    data['year'] = [d.split('/')[2] for d in data.date]
    data['month'] = [d.split('/')[0] for d in data.date]
    groupedData = pd.DataFrame()
    if(indicator == "monthly"):
        groupedData = data.groupby(["year","month"])
    elif(indicator == "yearly"):
        groupedData = data.groupby(["year"])
    output = pd.DataFrame()
    output['ShapRatio'] = (groupedData['% Gain/Loss'].mean()/groupedData['% Gain/Loss'].std())
    output['Total Positive Return %'] = groupedData['% Gain/Loss'].agg(lambda x: x[x>0].sum())
    output['Total Negative Return %'] = groupedData['% Gain/Loss'].agg(lambda x: x[x<0].sum())
    output['Total  Net % Return'] = ((groupedData['Act Balance'].agg(lambda x: x.iloc[-1]) - groupedData['Act Balance'].agg(lambda x: x.iloc[0]))/groupedData['Act Balance'].agg(lambda x: x.iloc[0]))*100
    output['% Vol'] = groupedData['% Gain/Loss'].std()
    output['Upward Vol'] = groupedData['% Gain/Loss'].agg(lambda x: x[x>0].std(ddof=0))
    output['Downward Volatality'] = groupedData['% Gain/Loss'].agg(lambda x: x[x<0].std(ddof=0))
    output['SortinoRatio'] = groupedData['% Gain/Loss'].mean()/output['Downward Volatality']
    output['# of downdays'] = groupedData['% Gain/Loss'].agg(lambda x: x[x<0].count())
    output['# of updays'] = groupedData['% Gain/Loss'].agg(lambda x: x[x>0].count())
    output['total counts'] = groupedData['% Gain/Loss'].agg(lambda x: x.count())
    output['% Downdays'] = (output['# of downdays'] / output['total counts'])*100
    output['% of updays'] = (output['# of updays'] / output['total counts'])*100
    
    if(indicator == "monthly"):
        output['Largest DailyGain'] = groupedData['% Gain/Loss'].max()
        output['Largest Daily Loss'] = groupedData['% Gain/Loss'].min()
        output['Monthly High Balance'] = groupedData['Act Balance'].max()
        output['Opening Bal'] = groupedData['Act Balance'].agg(lambda x: x.iloc[0])
        output['Closing Bal'] = groupedData['Act Balance'].agg(lambda x: x.iloc[-1])
    elif(indicator == "yearly"):
        if(sys.argv[3] != None and sys.argv[3] == "on"):
            output['Opening Bal'] = groupedData['Act Balance'].agg(lambda x: 100000)
            output['Closing Bal'] = ((output['Opening Bal']*output['Total  Net % Return'])/100+output['Opening Bal'])
        else: 
            output['Opening Bal'] = groupedData['Act Balance'].agg(lambda x: x.iloc[0])
            output['Closing Bal'] = groupedData['Act Balance'].agg(lambda x: x.iloc[-1])
        output['Largest Monthly Gain'] = groupedData['% Gain/Loss'].max()
        output['Largest Monthly Loss'] = groupedData['% Gain/Loss'].min()
        output['Yearly High Balance'] = groupedData['Act Balance'].max()
        output['Yearly Low Balance'] = groupedData['Act Balance'].min()
    
    
    output['Total Return(Market)'] =  (groupedData['close'].agg(lambda x: x.iloc[-1]) - groupedData['close'].agg(lambda x: x.iloc[0]))/groupedData['close'].agg(lambda x: x.iloc[0])*100
    output['Total Vol(Market)'] =  groupedData['% Change'].std()
    output['Up days(Market)'] = groupedData['% Change'].agg(lambda x: x[x>0].count())
    output['down days(Market)'] = groupedData['% Change'].agg(lambda x: x[x<0].count())
    
    return output
    
    
def main():
    
    try:
        data = pd.read_csv(sys.argv[1],thousands=',')
    except:
        print("File does not exist : "+sys.argv[1])
        sys.exit(0)
     
    monthlyStats = statsCalculate(data,"monthly")
    
    print("Writing monthly report.!")
    monthlyStats.reset_index().to_csv("monthly-"+sys.argv[2], index=False)

    yearlyStats = statsCalculate(data,"yearly")
    
    print("Writing yearly report.!")
    yearlyStats.reset_index().to_csv("yearly-"+sys.argv[2], index=False)


if __name__ == "__main__":
    
    start_time = time.time()
    
    if (len(sys.argv) < 4):
        print("""\
This script will produce the monthly and yearly summary reports.

Usage: python summaryStats.py inputDatacsvfile outputfile Reset-Balance(on or off)
            """)
        sys.exit(0)

    print("Started.!")  
    main()
    print("Summary Reports Generation Complete.!")
    print("--- %s seconds for execution ---" % (time.time() - start_time))