from django.conf.urls import url
from . import views

from rest_framework import routers

from rest_framework.urlpatterns import format_suffix_patterns


# Routers provide an easy way of automatically determining the URL conf.
router = routers.DefaultRouter()


urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^stats_report$', views.stats_report, name='stats_report'),
    url(r'^realtime_report$', views.realtime_report, name='realtime_report'),
    
    url(r'^api/monthly_sales_vol/(?P<year>.+)$', views.get_monthly_sales_volumn_data),
    url(r'^api/desc_total_sales_vol/(?P<year>.+)$', views.get_desc_total_sales_volumn),
    url(r'^api/monthly_product_cate_sales_amount/(?P<year>.+)$', views.get_monthly_total_amount_per_product_cate),
    url(r'^api/monthly_product_cate_detail_sales_amount/(?P<year>.+)$', views.get_monthly_total_amount_product_cate_detail),
    url(r'^api/timebase_sales_amount/(?P<year>.+)$', views.get_timebase_sales_amount_info),
    
    url(r'^api/test_func$', views.test_func),
    url(r'^api/get_most_popular_products$', views.get_most_popular_products),
    url(r'^api/get_product_price_map_data$', views.get_product_price_map_data),
    url(r'^api/req_write_transaction_log$', views.req_write_transaction_log),
    
    url(r'^api/get_timebase_data_on_past_specific_date/(?P<date>.+)$', views.get_timebase_data_on_past_specific_date),
    url(r'^api/get_timebase_data_on_today_specific_date/(?P<date>.+)$', views.get_timebase_data_on_today_specific_date),
    
]

urlpatterns = format_suffix_patterns(urlpatterns)