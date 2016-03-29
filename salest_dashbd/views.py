from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.decorators import parser_classes
from rest_framework.parsers import JSONParser
from rest_framework.response import Response

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
def get_monthly_sales_volumn_data(request):
    if request.method == 'GET':
        return Response("RESPONSE OK", status=status.HTTP_200_OK)

@api_view(['GET'])
def get_desc_total_sales_volumn(request):
    if request.method == 'GET':
        return Response("RESPONSE OK", status=status.HTTP_200_OK)

@api_view(['GET'])
def get_monthly_total_amount_per_product_cate(request):
    if request.method == 'GET':
        return Response("RESPONSE OK", status=status.HTTP_200_OK)

@api_view(['POST'])
@parser_classes((JSONParser,))
def get_monthly_total_amount_product_cate_detail(request,format=None):
    if request.method == 'POST':
        return Response("RESPONSE OK", status=status.HTTP_200_OK)
    
@api_view(['POST'])
@parser_classes((JSONParser,))
def get_timebase_sales_amount_info(request,format=None):
    if request.method == 'POST':
        return Response("RESPONSE OK", status=status.HTTP_200_OK)
    