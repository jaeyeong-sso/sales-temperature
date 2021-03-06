# coding: utf-8

import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
import ast

import redis_io as redis_io

from pandas.tseries.offsets import MonthEnd
from dateutil.parser import parse

import numpy as np

##############################################################################################################
# REDIS-KEY
##############################################################################################################
# monthly_sales_vol:{year}   << agg_montly_sales_volumn(year,unit_numofproduct, unit_totalamount)
# desc_total_sales_vol:{year}   << def desc_total_sales_volumn(year):
# monthly_total_amount_per_cate:{year}   << def agg_montly_total_amount_by_product_cate(year):
# monthly_total_amount_per_product:{year}:{cate}   << def agg_montly_total_amount_by_product(year, product_cate):
# timebase_sales_amount:{year}:{day_of_week}  << def analysis_timebase_sales_amount(year, day_of_week):
##############################################################################################################


def agg_montly_sales_volumn(year,unit_numofproduct, unit_totalamount):
    
    # Redis read cache value
    REDIS_KEY = "monthly_sales_vol:{0}".format(year)
    cached_data = redis_io.read_transaction(REDIS_KEY)
    
    if cached_data != None:
        return  ast.literal_eval(cached_data)
    #
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    
    bIsRealTimeUpdated = REFRESH_IMPALA_TABLE(cur, year)
    
    cur.execute("""
        SELECT year_month, SUM(num_of_product) AS num_of_product, SUM(sales_amount) AS total_amount
        FROM (
            SELECT SUBSTR(date_receipt_num,1,7) AS year_month, num_of_product, sales_amount
            FROM """ + GET_IMPALA_TB_NAME(year) + """ WHERE SUBSTR(date_receipt_num,1,4) = '""" + year +
        """'
        ) view_tr_recipt
        GROUP BY year_month ORDER BY year_month ASC
        """
    )
    df = as_pandas(cur)
    conn.close()
    
    ### Fill non-included monthly row with zero base values.
    month_index_arr = []

    for month in range(1,13):
        month_index_arr.append("{0}-{1:02d}".format(year,month))
    
    df_base_index = pd.DataFrame(data=month_index_arr, columns=['year_month'])
    df_all_monatly_sales_volume = pd.merge(df, df_base_index, on='year_month',how='outer').fillna(0).sort_values(by='year_month',ascending='1')
    ###

    df_list = list(df_all_monatly_sales_volume.itertuples(index=False))
    df_column_name_list = list(df.columns.values)

    list_month_sales_volume = []
    dict_month_sales_volume = {}

    for row in df_list:
        dict_month_sales_volume = {}
        
        for key,value in zip(df_column_name_list, row):
            if(key=='num_of_product'):
                value = int(round(value / unit_numofproduct))
            if(key=='total_amount'):
                value = int(round(value / unit_totalamount))
            dict_month_sales_volume[key] = value
        
        list_month_sales_volume.append(dict_month_sales_volume.copy())

    if bIsRealTimeUpdated == False:
        # Redis save cache value
        redis_io.write_transaction(REDIS_KEY, list_month_sales_volume)
        #
    
    return list_month_sales_volume


#################################################################################################################################
# Describe overall stat (Whole Sales Products/Amount , Average Sales Products/Amount per daily
#################################################################################################################################

def desc_total_sales_volumn(year):
    
    # Redis read cache value
    REDIS_KEY = "desc_total_sales_vol:{0}".format(year)
    cached_data = redis_io.read_transaction(REDIS_KEY)
    
    if cached_data != None:
        return ast.literal_eval(cached_data)
    #
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    # daily transaction agg
    cur.execute('USE salest')
    
    bIsRealTimeUpdated = REFRESH_IMPALA_TABLE(cur, year)
        
    cur.execute("""
        SELECT year_month_day, SUM(num_of_product) AS num_of_product, SUM(sales_amount) AS total_amount
        FROM (
            SELECT SUBSTR(date_receipt_num,1,10) AS year_month_day, num_of_product, sales_amount
            FROM """ + GET_IMPALA_TB_NAME(year) + """ WHERE SUBSTR(date_receipt_num,1,4) = '""" + year +
        """'
        ) view_tr_recipt
        GROUP BY year_month_day ORDER BY year_month_day ASC
        """
    )

    df_tr_agg_daily = as_pandas(cur)
    conn.close()
    
    series_sum = df_tr_agg_daily[['num_of_product','total_amount']].sum()
    series_sum.name = 'sum'

    df_desc = df_tr_agg_daily.describe().append(series_sum)
    df_desc['num_of_product'] = df_desc['num_of_product'].apply(lambda v: round(v))
    df_desc['total_amount'] = df_desc['total_amount'].apply(lambda v: round(v))
    
    df_desc.fillna(0, inplace=True)
    
    cached_data = df_desc.to_dict()
    
    if bIsRealTimeUpdated == False:
        # Redis save cache value
        redis_io.write_transaction(REDIS_KEY, cached_data)
        #
        cached_data = redis_io.read_transaction(REDIS_KEY)
        cached_data = ast.literal_eval(cached_data)
    
    return cached_data


#################################################################################################################################
def agg_montly_total_amount_by_product_cate(year):
    
    # Redis read cache value
    REDIS_KEY = "monthly_total_amount_per_cate:{0}".format(year)
    cached_data = redis_io.read_transaction(REDIS_KEY)
    
    if cached_data != None:
        return ast.literal_eval(cached_data)
    
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    
    bIsRealTimeUpdated = REFRESH_IMPALA_TABLE(cur, year)
        
    cur.execute(
    """
        SELECT SUBSTR(view_tr_receipt.date_receipt_num,1,7) AS year_month, 
              view_tr_receipt.num_of_product, view_tr_receipt.sales_amount AS total_amount,
            ext_menumap_info.product_name, ext_menumap_info.cate_name, ext_menumap_info.price
        FROM (SELECT * FROM """ + GET_IMPALA_TB_NAME(year) + """ WHERE SUBSTR(date_receipt_num,1,4) = '""" + year + "'" +
    """) view_tr_receipt JOIN ext_menumap_info USING (product_code)"""
    )

    df_tr_receipt_menumap = as_pandas(cur)
    conn.close()
    
    def aggregation(row):
        total_amount = row['total_amount'].sum()
        return pd.Series([total_amount], index=['total_amount'])
    
    df_monthly_product_tr = df_tr_receipt_menumap.groupby(['year_month','cate_name']).apply(aggregation)
    
    df_default = genDefaultMontlyCateTotalAmountDataFrame(df_monthly_product_tr,year, 'cate_name')
    df_all_monatly_sales_volume = pd.merge(df_default, df_monthly_product_tr, left_index=True, right_index=True, how='outer').fillna(0).sort_index(ascending='1')

    def post_aggregation(row):
        return row['total_amount_x'] + row['total_amount_y']
    
    df_all_monatly_sales_volume['total_amount'] = df_all_monatly_sales_volume.apply(post_aggregation, axis=1)
    df_all_monatly_sales_volume.drop(['total_amount_x','total_amount_y'], axis=1, inplace=True)

    def gen_dict_total_amount(month_rows):
        monthlyDict = {}
        monthlyDictKey = month_rows.index.get_level_values('year_month')[0]
        
        monthCateItemsStr = "{"
        for item in zip(month_rows.index.get_level_values('cate_name'),month_rows['total_amount']):
            monthCateItemsStr += "'{0}':{1},".format(item[0],item[1]);
        
        monthCateItemsStr = monthCateItemsStr[:-1]
        monthCateItemsStr += "}"
        
        monthlyDict = ast.literal_eval(monthCateItemsStr)
        monthlyDict['year_month'] = month_rows.index.get_level_values('year_month')[0]

        return monthlyDict
    
    mothlyTotalAmountDictItems = df_all_monatly_sales_volume.groupby(df_all_monatly_sales_volume.index.get_level_values('year_month')).apply(gen_dict_total_amount)    
 
    mothlyTotalAmountDict = {}
    mothlyTotalAmountList = []
    for item in mothlyTotalAmountDictItems:
        mothlyTotalAmountList.append(item)
    mothlyTotalAmountDict['total_amount'] = mothlyTotalAmountList

    if bIsRealTimeUpdated == False:
        # Redis save cache value
        redis_io.write_transaction(REDIS_KEY, mothlyTotalAmountDict)
        #
    
    return mothlyTotalAmountDict


#################################################################################################################################

def agg_montly_total_amount_by_product(year, product_cate):
    
    # Redis read cache value
    REDIS_KEY = "monthly_total_amount_per_product:{0}:{1}".format(year,product_cate.encode("UTF-8"))
    cached_data = redis_io.read_transaction(REDIS_KEY)
    
    if cached_data != None:
        return ast.literal_eval(cached_data)
    #
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()
    
    cur.execute('USE salest')
    
    bIsRealTimeUpdated = REFRESH_IMPALA_TABLE(cur, year)
    
    query_str = """
        SELECT * FROM (
            SELECT SUBSTR(view_tr_receipt.date_receipt_num,1,7) AS year_month, 
                  view_tr_receipt.num_of_product, view_tr_receipt.sales_amount AS total_amount,
                  ext_menumap_info.product_name, ext_menumap_info.cate_name, ext_menumap_info.price
            FROM (SELECT * FROM """ + GET_IMPALA_TB_NAME(year) + """ WHERE SUBSTR(date_receipt_num,1,4) = '%s'
            ) view_tr_receipt JOIN ext_menumap_info USING (product_code)
        ) view_tr_receipt_menumap
        WHERE cate_name = '%s'
        """  % (year,product_cate)
        
    cur.execute(query_str.encode("UTF-8"))
    
    df_monthly_product_tr = as_pandas(cur)
    conn.close()
        
    column_func_tuple = [('total_amount','sum')]
    df_monthly_summary = df_monthly_product_tr.groupby(['year_month','product_name'])['total_amount'].agg(column_func_tuple)
    df_monthly_summary.rename(columns={'total_amount': 'total_amount_B'}, inplace=True)

    df_default = genDefaultMontlyCateTotalAmountDataFrame(df_monthly_summary,year, 'product_name')
    df_default.rename(columns={'total_amount': 'total_amount_A'}, inplace=True)

    df_per_category = pd.concat([df_default, df_monthly_summary], axis=1).fillna(0)

    def post_aggregation(row):
        return row[0] + row[1]
    
    df_per_category['total_amount'] = df_per_category.apply(post_aggregation, axis=1)
    df_per_category.drop(['total_amount_A','total_amount_B'],axis=1,inplace=True)
  
     # Overall Top 10 menu items in category 
    
    df_topten_products_by_total_amount = df_monthly_product_tr.groupby(['product_name']).sum().sort_values(by='total_amount', ascending=False)[:10]
    df_topten_products_by_total_amount.drop(['num_of_product'],axis=1, inplace=True)
    df_topten_products_by_total_amount.rename(columns={'total_amount':'overall_total_amount'},inplace=True)

    # Redis save cache value
    redis_io.write_transaction(product_cate, df_topten_products_by_total_amount.index.tolist(), 60*60*24*7)
    #
    
    # Merge the above two dataframes
    df_new = df_per_category.reset_index(level=0)
    df_merged = pd.merge(df_new, df_topten_products_by_total_amount, left_index=True, right_index=True, how='left').sort_values(by='year_month', ascending=True)
    
    def agg_monthly_items_summary(row):
        sr_columns = row[row['overall_total_amount'].notnull()].index
        sr_values = row[row['overall_total_amount'].notnull()]['total_amount']

        etcSum = row[row['overall_total_amount'].isnull()]['total_amount'].sum()

        sr_columns = sr_columns.insert(sr_columns.size,'ETC')
        sr_etc = pd.Series([etcSum], index=['ETC'])
        sr_values = sr_values.append(sr_etc)

        return pd.Series(sr_values, index=sr_columns)
    
    df_merged_new = df_merged.reset_index(level=0)

    df_agg_monthly_summary = df_merged.groupby(['year_month']).apply(agg_monthly_items_summary)#.unstack()
    df_agg_monthly_summary.fillna(0,inplace=True)
    
    monthlyDictItems = df_agg_monthly_summary.apply(gen_dict_total_amount,axis=1)
    
    mothlyTotalAmountDict = {}
    mothlyTotalAmountList = []
    for item in monthlyDictItems:
        mothlyTotalAmountList.append(item)
    mothlyTotalAmountDict['total_amount'] = mothlyTotalAmountList
    
    if bIsRealTimeUpdated == False:
        # Redis save cache value
        redis_io.write_transaction(REDIS_KEY, mothlyTotalAmountDict)
        #
    
    return mothlyTotalAmountDict


#################################################################################################################################
# day_of_week : W-MON, W-TUE, W-WED, W-THU, W-FRI, W-SAT, W-SUN
#################################################################################################################################
def analysis_timebase_sales_amount(year, day_of_week):
    
    # Redis read cache value
    REDIS_KEY = "timebase_sales_amount:{0}:{1}".format(year,day_of_week)
    cached_data = redis_io.read_transaction(REDIS_KEY)
    
    if cached_data != None:
        return ast.literal_eval(cached_data)
    #
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    
    bIsRealTimeUpdated = REFRESH_IMPALA_TABLE(cur, year)
        
    start_date = "%s/01/01" % year
    end_date = "%s/12/31" % year
    
    if(day_of_week=='All'):
        target_date_idx = pd.date_range(start_date,end_date)
    else:
        target_date_idx = pd.date_range(start_date,end_date, freq=day_of_week)
        
    target_date_arr = target_date_idx.strftime('%Y-%m-%d')
    target_date_tuple = tuple(target_date_arr)

    cur.execute(
        """
        SELECT time_hour, CAST(SUM(sales_amount) as INTEGER) AS total_amount, 
        COUNT(sales_amount) as num_of_transaction,
        COUNT(DISTINCT year_month_day) as date_count
        FROM(
            SELECT SUBSTR(date_receipt_num,1,10) AS year_month_day,
            SUBSTR(tr_time,1,2) AS time_hour,
            sales_amount
            FROM """ + GET_IMPALA_TB_NAME(year) + """ WHERE SUBSTR(date_receipt_num,1,10) IN %s
            """ % (target_date_tuple,) +
            """
        ) view_tr_total_amount_by_dayofweek
        GROUP BY time_hour ORDER BY time_hour ASC
        """
    )
    df_by_weekofday = as_pandas(cur)
    conn.close()
        
    def calc_average_amount(row):
        return row.total_amount / row.date_count

    df_by_weekofday['total_amount'] = df_by_weekofday.apply(calc_average_amount,axis=1)
    df_by_weekofday.set_index('time_hour',inplace=True)
    
    cached_data = df_by_weekofday.to_dict()
    
    if bIsRealTimeUpdated == False:
        # Redis save cache value
        redis_io.write_transaction(REDIS_KEY, cached_data)
        #
    
    return cached_data


def get_most_popular_products(req_cate_name):

        
    # Redis read cache value
    REDIS_KEY = "product_category_info"
    
    def get_cache_value(req_cate_name):
        subKeyList = []
        subKeyList.append(req_cate_name)
        return redis_io.read_dict_transaction(REDIS_KEY, subKeyList)

        
    # Redis read cache value
    #REDIS_KEY = "product_category_info"
    #cached_data = redis_io.read_dict_transaction(REDIS_KEY,req_cate_name)

    #if cached_data != None:
    #    return ast.literal_eval(cached_data[0])
    cached_data = get_cache_value(req_cate_name)
    if cached_data != None:
        return ast.literal_eval(cached_data[0])
    
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()
    cur.execute('USE salest')

    dict_product_cate_items = {}

    # Category Items

    queryStr = """
            SELECT DISTINCT cate_name FROM ext_menumap_info
            """

    cur.execute(queryStr)
    df_categories = as_pandas(cur)
    dict_product_cate_items['All'] = df_categories['cate_name'].values.tolist()


    # Most 10 papular items per each category

    for cate_name in dict_product_cate_items['All']:
        query_str = """
            SELECT product_name, SUM(sales_amount) AS total_amount
            FROM
            (
                SELECT cate_name,product_name,date_receipt_num,sales_amount
                FROM 
                    (SELECT * FROM ext_menumap_info WHERE cate_name = '""" + cate_name + """' ) view_specific_menu JOIN ext_tr_receipt USING (product_code)
            ) view_tr_specific_cate_menu
            GROUP BY (view_tr_specific_cate_menu.product_name)
            ORDER BY (SUM(sales_amount)) DESC
            LIMIT 10
            """
        cur.execute(query_str)
  
        df_papular_products = as_pandas(cur)
        df_papular_products = df_papular_products[df_papular_products.total_amount != 0]
        dict_product_cate_items[cate_name] = df_papular_products['product_name'].values.tolist()

    conn.close()

    # Redis save cache value
    redis_io.write_dict_transaction(REDIS_KEY, dict_product_cate_items, 60*60*24*30)
    #
    cached_data = get_cache_value(req_cate_name)
    return ast.literal_eval(cached_data[0])


def get_product_data(product_name):
    
    # Redis read cache value
    REDIS_KEY_PREFIX = "popular_product_info"
    
    def get_cache_value(product_name):
        cache_data = redis_io.read_dict_transaction(REDIS_KEY_PREFIX + ":" + product_name, ['product_code','price'])
        if cache_data == None:
            return None
    
        dict_data = {}
        dict_data['product_code'] = cache_data[0]
        dict_data['price'] = cache_data[1]
        return dict_data
    

    cached_data = get_cache_value(product_name)
    if cached_data != None:
        return cached_data
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()
    cur.execute('USE salest')
  
    queryStr = """SELECT product_name,price,product_code FROM ext_menumap_info"""
    
    cur.execute(queryStr)
    df_categories = as_pandas(cur)

    df_categories = df_categories[df_categories.price != 0]
    #df_categories.set_index('product_name', inplace=True)
    
    for idx,row in df_categories.iterrows():
        key = "{0}:{1}".format(REDIS_KEY_PREFIX,row.product_name)
        value = row[['product_code','price']].to_dict()
        redis_io.write_dict_transaction(key, value, 60*60*24*30)
    
    cached_data = redis_io.read_dict_transaction(REDIS_KEY_PREFIX + ":" + product_name, ['product_code','price'])
    return get_cache_value(product_name)



def get_timebase_data_on_past_specific_date(cur_date):
    
    # Redis read cache value
    REDIS_KEY_PREFIX = "past_timebase_data_of"
    
    def get_cache_value(cur_date):
        return redis_io.read_dict_transaction(REDIS_KEY_PREFIX + ":" + cur_date) 
    
    cached_data = get_cache_value(cur_date)
    if cached_data != None:
        return cached_data
    
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    
    date_list = tuple(get_past_target_date(cur_date))

    cur.execute(
    """
        SELECT time_hour, CAST(SUM(sales_amount) as INTEGER) AS total_amount, 
        COUNT(sales_amount) as num_of_transaction,
        COUNT(DISTINCT year_month_day) as date_count
        FROM(
            SELECT SUBSTR(date_receipt_num,1,10) AS year_month_day,
            SUBSTR(tr_time,1,2) AS time_hour,
            sales_amount
            FROM ext_tr_receipt WHERE SUBSTR(date_receipt_num,1,10) IN ('%s')
            """ % date_list +
            """
        ) view_tr_total_amount_by_dayofweek
        GROUP BY time_hour ORDER BY time_hour ASC
        """
    )
    df_by_hour = as_pandas(cur)
    conn.close()
    
    df_by_hour.set_index('time_hour',inplace=True)
    df_by_hour = df_by_hour.reindex([[str(i) for i in np.arange(10,24)]],fill_value=0)

    dict_result = df_by_hour['total_amount'].to_dict()
    dict_result['date'] = date_list[0]
     
    redis_io.write_dict_transaction(REDIS_KEY_PREFIX + ":" + cur_date, dict_result, 60*60)
    
    ret_dict = get_cache_value(cur_date)

    return ret_dict


def get_timebase_data_on_today_specific_date(cur_date):
    
    # Redis read cache value
    REDIS_KEY_PREFIX = "today_timebase_data_of"
    
    dictData = {}
    
    for time_idx in np.arange(10,24):
        cache_value = redis_io.read_dict_transaction("{0}:{1}:{2}".format(REDIS_KEY_PREFIX,cur_date,time_idx))
        if cache_value == None:
            dictData[str(time_idx)] = str(0)
        else :
            if len(cache_value) > 0:
                dictData[str(time_idx)] = cache_value['total_amount']
            else:
                dictData[str(time_idx)] = str(0)
        
    return dictData

    
#################################################################################################################################
# Utility Functions
#################################################################################################################################

def gen_dict_total_amount(row):
    
    monthlyDict = {}
    monthlyDictStr = "{"
    for key,value in zip(row.index, row): 
        monthlyDictStr += "'{0}':{1},".format(key,value)
    
    monthlyDictStr = monthlyDictStr[:-1]
    monthlyDictStr += "}"
    
    monthlyDict = ast.literal_eval(monthlyDictStr)
    monthlyDict['year_month'] = row.name
 
    return monthlyDict


def genDefaultMontlyCateTotalAmountDataFrame(df_monthly_product_tr,year, second_index_name):
    unique_count_of_category = len(df_monthly_product_tr.index.get_level_values(1).unique())

    month_index_arr = []
    cate_index_arr = []

    for month in range(1,13):
        for cate in range(unique_count_of_category):
            month_index_arr.append("{0}-{1:02d}".format(year,month))

    unique_cate_index_arr = df_monthly_product_tr.index.get_level_values(1).unique()
    #df_monthly_product_tr.ix[0:unique_count_of_category].index.get_level_values(1)

    for month in range(1,13):
        for cate in unique_cate_index_arr:
            cate_index_arr.append(cate)

    full_month_cate_multi_index = pd.MultiIndex.from_tuples(zip(month_index_arr, cate_index_arr), 
                                                  names=['year_month', second_index_name])
    df_full_month_cate_default = pd.DataFrame(0, index=full_month_cate_multi_index, columns=['total_amount'])
    return df_full_month_cate_default

def get_past_target_date(date_year_mon_day):
    
    day_of_week_map = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']

    date_val = parse(date_year_mon_day)

    last_day_of_month = (date_val + MonthEnd()).day
    week_number = (date_val.day - 1) // 7 + 1
    
    freq_faram = "WOM-{0}{1}".format(week_number, day_of_week_map[date_val.weekday()])

    last_year_day_start = "{0}-{1}-01".format(date_val.year-1, date_val.month)
    last_year_day_end =  "{0}-{1}-{2}".format(date_val.year-1, date_val.month, last_day_of_month)

    target_date_idx = pd.date_range(last_year_day_start, last_year_day_end, freq=freq_faram)

    date_list = target_date_idx.strftime('%Y-%m-%d').tolist()
    return date_list

def GET_IMPALA_TB_NAME(year):
    if year == '2016':
        return'tmp_ext_tr_receipt'
    else:
        return 'ext_tr_receipt'
    
def REFRESH_IMPALA_TABLE(cursor, year):
    if year == '2016':
        cursor.execute("REFRESH tmp_ext_tr_receipt")
        return True
    else :
        return False
        