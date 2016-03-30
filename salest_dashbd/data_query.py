import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
import ast

def agg_montly_sales_volumn(unit_numofproduct, unit_totalamount):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    cur.execute("""
        SELECT year_month, SUM(num_of_product) AS num_of_product, SUM(sales_amount) AS total_amount
        FROM (
            SELECT SUBSTR(date_receipt_num,1,7) AS year_month, num_of_product, sales_amount
            FROM ext_tr_receipt
        ) view_tr_recipt
        GROUP BY year_month ORDER BY year_month ASC
    """)
    df = as_pandas(cur)

    df_list = list(df.itertuples(index=False))
    df_column_name_list = list(df.columns.values)

    list_month_sales_volume = []
    dict_month_sales_volume = {}

    for row in df_list:
        dict_month_sales_volume = {}
        
        for key,value in zip(df_column_name_list, row):
            if(key=='num_of_product'):
                value = value / unit_numofproduct
            if(key=='total_amount'):
                value = value / unit_totalamount
            dict_month_sales_volume[key] = value
        
        list_month_sales_volume.append(dict_month_sales_volume.copy())

    conn.close()
    return list_month_sales_volume


#################################################################################################################################
# Describe overall stat (Whole Sales Products/Amount , Average Sales Products/Amount per daily
#################################################################################################################################

def desc_total_sales_volumn():
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    # daily transaction agg
    cur.execute('USE salest')
    cur.execute("""
    SELECT year_month_day, SUM(num_of_product) AS num_of_product, SUM(sales_amount) AS total_amount
    FROM (
        SELECT SUBSTR(date_receipt_num,1,10) AS year_month_day, num_of_product, sales_amount
        FROM ext_tr_receipt
    ) view_tr_recipt
    GROUP BY year_month_day ORDER BY year_month_day ASC
    """)

    df_tr_agg_daily = as_pandas(cur)
    conn.close()
    
    series_sum = df_tr_agg_daily[['num_of_product','total_amount']].sum()
    series_sum.name = 'sum'

    df_desc = df_tr_agg_daily.describe().append(series_sum)
    df_desc['num_of_product'] = df_desc['num_of_product'].apply(lambda v: round(v))
    df_desc['total_amount'] = df_desc['total_amount'].apply(lambda v: round(v))
    
    return df_desc.to_dict()

#################################################################################################################################
def agg_montly_total_amount_by_product_cate():
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    cur.execute(
    """
    SELECT SUBSTR(ext_tr_receipt.date_receipt_num,1,7) AS year_month, ext_tr_receipt.num_of_product, ext_tr_receipt.sales_amount AS total_amount,
    ext_menumap_info.product_name, ext_menumap_info.cate_name, ext_menumap_info.price
    FROM ext_tr_receipt JOIN ext_menumap_info USING (product_code)
    """)
    df_tr_receipt_menumap = as_pandas(cur)
    
    def aggregation(row):
        total_amount = row['total_amount'].sum()
        return pd.Series([total_amount], index=['total_amount'])
    
    df_monthly_product_tr = df_tr_receipt_menumap.groupby(['year_month','cate_name']).apply(aggregation)

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


    mothlyTotalAmountDictItems = df_monthly_product_tr.groupby(df_monthly_product_tr.index.get_level_values('year_month')).apply(gen_dict_total_amount)

    mothlyTotalAmountDict = {}
    mothlyTotalAmountList = []
    for item in mothlyTotalAmountDictItems:
        mothlyTotalAmountList.append(item)
    mothlyTotalAmountDict['total_amount'] = mothlyTotalAmountList

    return mothlyTotalAmountDict;


#################################################################################################################################

def agg_montly_total_amount_by_product(product_cate):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    cur.execute(
        """
        SELECT * FROM (
            SELECT SUBSTR(ext_tr_receipt.date_receipt_num,1,7) AS year_month, 
                ext_tr_receipt.num_of_product AS num_of_product, ext_tr_receipt.sales_amount AS total_amount,
                ext_menumap_info.product_name AS product_name, ext_menumap_info.cate_name AS cate_name, ext_menumap_info.price
            FROM ext_tr_receipt JOIN ext_menumap_info USING (product_code)
        ) view_tr_receipt_menumap
        WHERE cate_name =
        """ + "'" + product_cate.encode('utf8') + "'"
    )
    
    df_category = as_pandas(cur)

    column_func_tuple = [('total_amount','sum')]
    df_monthly_product_tr = df_category.groupby(['year_month','product_name'])['total_amount'].agg(column_func_tuple)

    # Overall Top 10 menu items in category 
    
    df_topten_products_by_total_amount = df_category.groupby(['product_name']).sum().sort_values(by='total_amount', ascending=False)[:10]
    df_topten_products_by_total_amount.drop(['num_of_product'],axis=1, inplace=True)
    df_topten_products_by_total_amount.rename(columns={'total_amount':'overall_total_amount'},inplace=True)

    # Merge the above two dataframes
    df_new = df_monthly_product_tr.reset_index(level=0)
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
    df_merged_new.groupby(['year_month'])

    df_agg_monthly_summary = df_merged.groupby(['year_month']).apply(agg_monthly_items_summary).unstack()
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
def analysis_timebase_sales_amount(day_of_week):
    
    conn = connect(host='salest-master-server', port=21050)
    cur = conn.cursor()

    cur.execute('USE salest')
    
    if(day_of_week=='All'):
        target_date_idx = pd.date_range("2014/12/01","2015/12/31")
    else:
        target_date_idx = pd.date_range("2014/12/01","2015/12/31", freq=day_of_week)
        
    target_date_arr = target_date_idx.strftime('%Y-%m-%d')
    target_date_tuple = tuple(target_date_arr)

    cur.execute(
        """
        SELECT time_hour, CAST(SUM(sales_amount) as INTEGER) AS total_amount, COUNT(sales_amount)
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
    
    def calc_average_amount(row):
        return row.total_amount / len(target_date_tuple)

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
