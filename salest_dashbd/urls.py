from django.conf.urls import url
from . import views

from rest_framework import routers

from rest_framework.urlpatterns import format_suffix_patterns


# Routers provide an easy way of automatically determining the URL conf.
router = routers.DefaultRouter()


urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^api/monthly_sales_vol/(?P<year>.+)$', views.get_monthly_sales_volumn_data),
    url(r'^api/desc_total_sales_vol/(?P<year>.+)$', views.get_desc_total_sales_volumn),
    url(r'^api/monthly_product_cate_sales_amount/(?P<year>.+)$', views.get_monthly_total_amount_per_product_cate),
    url(r'^api/monthly_product_cate_detail_sales_amount/(?P<year>.+)$', views.get_monthly_total_amount_product_cate_detail),
    url(r'^api/timebase_sales_amount$', views.get_timebase_sales_amount_info),
]

urlpatterns = format_suffix_patterns(urlpatterns)