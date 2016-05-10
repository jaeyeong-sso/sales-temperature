#-*- coding: utf-8 -*-

from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser
from rest_framework.response import Response
from rest_framework.renderers import JSONRenderer

from rest_framework import status
from rest_framework import serializers

import data_query as dao
import logging
import redis_io as redis_io
import threading

from SalesTemperature.tasks import KafkaConsumerTask

logger = logging.getLogger(__name__)

class SerPostRequestParam(serializers.Serializer):
    category = serializers.CharField()
    def getCategory(self):
        return self.category
    
    
# Create your views here.
#def index(request):
#    return render(request, 'salest_dashbd/index.html')

def index(request):
    return stats_report(request)

def stats_report(request):
    return render(request, 'salest_dashbd/stats_report.html')

def realtime_report(request):
    return render(request, 'salest_dashbd/realtime_report.html')  
    
@api_view(['GET'])
def get_monthly_sales_volumn_data(request,year):
    if request.method == 'GET':
        list_df = dao.agg_montly_sales_volumn(year,1,10000)
        content = JSONRenderer().render(list_df)
        return Response(content, status=status.HTTP_200_OK)

@api_view(['GET'])
def get_desc_total_sales_volumn(request,year):
    if request.method == 'GET':
        desc_dict = dao.desc_total_sales_volumn(year)
        content = JSONRenderer().render(desc_dict)
        return Response(content, status=status.HTTP_200_OK)

@api_view(['GET'])
def get_monthly_total_amount_per_product_cate(request,year):
    if request.method == 'GET':
        dictData = dao.agg_montly_total_amount_by_product_cate(year)
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)

@api_view(['POST'])
@parser_classes((JSONParser,))
def get_monthly_total_amount_product_cate_detail(request,year,format=None):
    if request.method == 'POST':
        cateReqParam = request.data['category']
        if cateReqParam == 'All':
            dictData = dao.agg_montly_total_amount_by_product_cate(year)
        else:
            dictData = dao.agg_montly_total_amount_by_product(year, cateReqParam)       
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
    
@api_view(['POST'])
@parser_classes((JSONParser,))
def get_timebase_sales_amount_info(request,year,format=None):
    if request.method == 'POST':
        dayOfWeekReqParam = request.data['dayOfWeek']
        dictData = dao.analysis_timebase_sales_amount(year,dayOfWeekReqParam)
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
@api_view(['POST'])
def test_func(request):
    return 

@api_view(['POST'])
@parser_classes((JSONParser,))
def get_most_popular_products(request):
    if request.method == 'POST':
        cateReqParam = request.data['category']
        dictData = dao.get_most_popular_products(cateReqParam)
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
@api_view(['POST'])
@parser_classes((JSONParser,))
def get_product_price_map_data(request):
    if request.method == 'POST':
        cateReqParam = request.data['product_name']
        dictData = dao.get_product_data(cateReqParam)
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
    
@api_view(['POST'])
@parser_classes((JSONParser,))
def req_write_transaction_log(request):
    if request.method == 'POST':
        productName = request.data['product_name']
        dictData = dao.get_product_data(productName)
        
        trDate = request.data['tr_date']
        trTime = request.data['tr_time']
        
        trCount = redis_io.get_transaction_incr_counter(trDate)
        # 2014-09-01-01,10:21:55,5,1,2500
        msg = "{0}-{1:02d},{2},{3},{4},{5}".format(trDate,int(trCount),trTime,dictData['product_code'],1,dictData['price'])
        logger.error(msg)
        
        return Response("", status=status.HTTP_200_OK)

@api_view(['GET'])
def get_timebase_data_on_past_specific_date(request,date):
    if request.method == 'GET':
        
        EventConsumerThread().start()
        
        pastDictData = dao.get_timebase_data_on_past_specific_date(date)
        todayDictData = dao.get_timebase_data_on_today_specific_date(date)
        
        list_merged_data = []

        for key in todayDictData.keys():
            dict_merged_data = {}
            dict_merged_data['H'] = key
            dict_merged_data['past'] = pastDictData[key]
            dict_merged_data['today'] = todayDictData[key]
            list_merged_data.append(dict_merged_data)
            
        dict_result = {}
        dict_result[pastDictData['date']] = list_merged_data
        
        content = JSONRenderer().render(dict_result)
        return Response(content, status=status.HTTP_200_OK)


@api_view(['GET'])
def get_timebase_data_on_today_specific_date(request,date):
    if request.method == 'GET':
        dictData = dao.get_timebase_data_on_today_specific_date(date)
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
    
class EventConsumerThread(threading.Thread):
    def run(self):
        KafkaConsumerTask.run()
