# coding: utf-8

import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
import ast
import math

def agg_montly_sales_volumn(year,unit_numofproduct, unit_totalamount):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    cur.execute("""
        SELECT year_month, SUM(num_of_product) AS num_of_product, SUM(sales_amount) AS total_amount
        FROM (
            SELECT SUBSTR(date_receipt_num,1,7) AS year_month, num_of_product, sales_amount
            FROM ext_tr_receipt WHERE SUBSTR(date_receipt_num,1,4) = '""" + year +
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

    return list_month_sales_volume


#################################################################################################################################
# Describe overall stat (Whole Sales Products/Amount , Average Sales Products/Amount per daily
#################################################################################################################################

def desc_total_sales_volumn(year):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    # daily transaction agg
    cur.execute('USE salest')
    cur.execute("""
        SELECT year_month_day, SUM(num_of_product) AS num_of_product, SUM(sales_amount) AS total_amount
        FROM (
            SELECT SUBSTR(date_receipt_num,1,10) AS year_month_day, num_of_product, sales_amount
            FROM ext_tr_receipt WHERE SUBSTR(date_receipt_num,1,4) = '""" + year +
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
    
    return df_desc.to_dict()

#################################################################################################################################
def agg_montly_total_amount_by_product_cate(year):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    cur.execute(
    """
        SELECT SUBSTR(view_tr_receipt.date_receipt_num,1,7) AS year_month, 
              view_tr_receipt.num_of_product, view_tr_receipt.sales_amount AS total_amount,
            ext_menumap_info.product_name, ext_menumap_info.cate_name, ext_menumap_info.price
        FROM (SELECT * FROM ext_tr_receipt WHERE SUBSTR(date_receipt_num,1,4) = '""" + year + "'" +
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

    return mothlyTotalAmountDict


#################################################################################################################################

def agg_montly_total_amount_by_product(year, product_cate):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()
    
    cur.execute('USE salest')
    
    query_str = """
        SELECT * FROM (
            SELECT SUBSTR(view_tr_receipt.date_receipt_num,1,7) AS year_month, 
                  view_tr_receipt.num_of_product, view_tr_receipt.sales_amount AS total_amount,
                  ext_menumap_info.product_name, ext_menumap_info.cate_name, ext_menumap_info.price
            FROM (SELECT * FROM ext_tr_receipt WHERE SUBSTR(date_receipt_num,1,4) = '%s'
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
    
    return mothlyTotalAmountDict


#################################################################################################################################
# day_of_week : W-MON, W-TUE, W-WED, W-THU, W-FRI, W-SAT, W-SUN
#################################################################################################################################
def analysis_timebase_sales_amount(year, day_of_week):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    
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
            FROM ext_tr_receipt WHERE SUBSTR(date_receipt_num,1,10) IN %s
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
    
    return df_by_weekofday.to_dict()



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
