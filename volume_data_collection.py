import pandas as pd
import json
import os
import time
from pathlib import Path

def get_price(coin,days,output_fp):
  print("Getting data for:",coin)
  rawfp = output_fp+"/raw/{}.txt".format(coin)
  url = "https://api.coingecko.com/api/v3/coins/{}/market_chart?vs_currency=usd&days={}&interval=daily".format(coin,days)
  req = "curl -X 'GET' '{}' -H 'accept: application/json' > {}".format(url,rawfp)
  os.system(req)
  df = pd.DataFrame(columns=['timestamp', 'price'])
  with open(rawfp,"r") as f:
      data = json.load(f)
  if "error" in data:
    return data
  else:
    for i in data["prices"]:
        df = df.append({"timestamp":float(i[0]),"price":float(i[1])}, ignore_index=True)
    df["day"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms').day)
    df["month"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms').month)
    df["year"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms').year)
    cfp = "{}/csv/{}.csv".format(output_fp,coin)
    df.to_csv(cfp, index=False)    
    return {"response":"success"}

def get_volume(ex_id,days,input_fp,output_fp):
  print("Getting data for:",ex_id)
  fp="{}/raw/{}.txt".format(input_fp,ex_id)
  url = "https://api.coingecko.com/api/v3/exchanges/{}/volume_chart?days={}".format(ex_id,days)
  req = "curl -X 'GET' '{}' -H 'accept: application/json' > {}".format(url,fp)
  os.system(req)
  df = pd.DataFrame(columns=['timestamp', 'volume'])
  with open(fp,"r") as f:
      data = json.load(f)
  if "error" in data:
    return data
  else:
    for i in data:
        df = df.append({"timestamp":float(i[0]),"volume":float(i[1])}, ignore_index=True)
    df["day"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms').day)
    df["month"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms').month)
    df["year"] = df["timestamp"].apply(lambda x : pd.to_datetime(x, unit='ms').year)
    cfp = "{}/csv/{}.csv".format(output_fp,ex_id)
    df.to_csv(cfp, index=False)    
    return {"response":"success"}

def get_usd_volume(price_fp,vol_fp):
  df1 = pd.read_csv(price_fp)
  df2 = pd.read_csv(vol_fp)
  df = pd.merge(df1,df2,on=["day","month","year"],suffixes=("_price","_vol"))
  df["usd_vol"] = df["volume"]*df["price"]
  return df

def get_data_looper(ex_id,days,input_fp,output_fp):
  notdone = True
  while notdone:
    resp = get_volume(ex_id,days,input_fp,output_fp)
    if "error" in resp:
      print("Error:",resp)
      days=days-1 # reduces one day from query if the API is unable to serve the request 
    else:
      notdone = False

def convert_to_usd_volume(coin,price_data_fp,vol_data_fp,output_fp):
  files = os.listdir(vol_data_fp)
  price_data_fp = price_data_fp+"/"+coin+".csv"
  for file in files:
    fp = vol_data_fp+"/"+file
    volume_data = get_usd_volume(price_data_fp,fp)
    volume_data.to_csv("{}/{}".format(output_fp,file),index=False)
  return 

def agg_all_exchange(ids,in_fp,out_fp):
  merged_data = pd.DataFrame(columns=["exchange","day",'month','year','volume','usd_vol'])
  for file in ids:
    fp = in_fp+"/"+file+".csv"
    merged = pd.read_csv(fp)
    merged["exchange"] = file
    merged_data = merged_data.append(merged,ignore_index=True) 
  merged_data.to_csv("{}/merged_data.csv".format(out_fp),index=False)
  return

def check_all_files_downloaded(ids,folder):
  path_read_files="./{}".format(folder)
  files = os.listdir(path_read_files)
  filesn = []
  for file in files:
    filesn.append(file[:-4])
  if len(set(ids).intersection(set(filesn)))>=len(ids):
    return True
  else:
    return False

def file_check(ids,folder):
  if check_all_files_downloaded(ids,folder):
    print("success")
  else:
    print("fail")

def main():
  ids = ["binance","ftx"] # add the exchange ids you want to get data for
  output_folder = "./myexchange" # add the folder you want to save the data to
  if os.path.exists(output_folder):
    print("folder exists please delete it")
    return
  else:
    pass
  days = 500 # number of days to get data for
  Path("{}/price/raw".format(output_folder)).mkdir(parents=True, exist_ok=True)
  Path("{}/price/csv".format(output_folder)).mkdir(parents=True, exist_ok=True)
  Path("{}/volume/raw".format(output_folder)).mkdir(parents=True, exist_ok=True)
  Path("{}/volume/csv".format(output_folder)).mkdir(parents=True, exist_ok=True)
  price_data_path= "{}/price".format(output_folder)
  vol_data_path = "{}/volume".format(output_folder)
  get_price("bitcoin",days,price_data_path) # get price data for bitcoin
  for i in ids:
    get_data_looper(i,days,input_fp=vol_data_path,output_fp=vol_data_path)
  if check_all_files_downloaded(ids,vol_data_path+"/csv"):
    convert_to_usd_volume(coin="bitcoin",price_data_fp=price_data_path+"/csv",vol_data_fp=vol_data_path+"/csv",output_fp=vol_data_path+"/csv")
  else:
    print("failed to download all files please retry")
  agg_all_exchange(ids,in_fp=vol_data_path+"/csv",out_fp=output_folder)
if __name__ == "__main__":
  main()


