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

class SerPostRequestParam(serializers.Serializer):
    category = serializers.CharField()
    def getCategory(self):
        return self.category
    
    
# Create your views here.
def index(request):
    return render(request, 'salest_dashbd/index.html')


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
        if cateReqParam == 'Category':
            dictData = dao.agg_montly_total_amount_by_product_cate()
        else:
            dictData = dao.agg_montly_total_amount_by_product(year, cateReqParam)       
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
    
@api_view(['POST'])
@parser_classes((JSONParser,))
def get_timebase_sales_amount_info(request,format=None):
    if request.method == 'POST':
        reqParam = request.data['dayOfWeek']
        dictData = dao.analysis_timebase_sales_amount(reqParam)
        content = JSONRenderer().render(dictData)
        return Response(content, status=status.HTTP_200_OK)
    
