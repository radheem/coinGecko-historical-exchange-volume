import os
import pandas as pd

def exchnages_in_path(input_fp):
    exchanges=[]
    files = os.listdir(input_fp)
    for file in files:
        name = file[:-4].split("_")
        if name[-1] =="agg":
            rmn = -8
        elif name[-1] =="merge":
            rmn = -10
            file=file[:rmn]
            exchanges.append(file)
        else:
            continue
    return exchanges

def agg_all_exchange(ids,in_fp,out_fp):
  """
  This fetches all the exchange data and aggregates them into one file
  The function creates two files with the following feilds:
    1. "exchange","day",'month','year','volume','usd_vol'
    2. "exchange",'month','year','volume','usd_vol'
  inputs:
    ids: list of exchange ids
    in_fp: input file path
    out_fp: output file path
  """
  merged_data = pd.DataFrame(columns=["exchange","day",'month','year','volume','usd_vol'])
  agg_data = pd.DataFrame(columns=["exchange",'month','year','volume','usd_vol'])
  for file in ids:
    fp = in_fp+"/"+file+"_merge.csv"
    merged = pd.read_csv(fp)
    fp = in_fp+"/"+file+"_agg.csv"
    agg = pd.read_csv(fp)
    merged["exchange"] = file
    merged_data = merged_data.append(merged,ignore_index=True)
    agg["exchange"] = file
    agg_data = agg_data.append(agg,ignore_index=True) 
  merged_data.to_csv("{}/merged_data.csv".format(out_fp),index=False)
  agg_data.to_csv("{}/agg_data.csv".format(out_fp),index=False)
  return

def exchages_per_year(df):
    """
        This function returns a dataframe of the number of exchanges per year in coin gecko. 
    """
    x = df.groupby(["year"])["exchange"].unique().reset_index()
    x["count"] = x["exchange"].apply(lambda x : len(x))
    return x

def remove_outlier_indices(x,lower_q,upper_q):
    Q1 = x.quantile(lower_q)
    Q3 = x.quantile(upper_q)
    IQR = Q3 - Q1 # gets the interquartile range
    falseList = ((x < (Q1 - 1.5 * IQR)) |(x > (Q3 + 1.5 * IQR))) # gets the indices of values that are outside the range
    return falseList

def pre_processing(df,l_q,u_q):
    """
        This pre processes CoinGecko exchange volume data. It does the following:
        1. adds date
        2. removes outliers
        return 
    """
    df["date"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms'))
    mylist = remove_outlier_indices(df["volume"],l_q,u_q)
    df["outlier"] = mylist
    df = df[df["outlier"] == False]
    return df

def main():
    return 

if __name__ == "__main__":
    main()