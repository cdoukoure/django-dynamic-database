from django.conf.urls import url

from .views import TableList, TableDetail, TableRowList, TableRowDetail

# app_name = 'django_dynamic_database'

urlpatterns = [
    url(
        r'^tables/$',
        TableList.as_view(),
        name='tables'
    ),
    url(
        r'^tables/(?P<pk>\d+)/$',
        TableDetail.as_view(),
        name='table-details'
    ),
    url(
        r'^tables/(?P<table_id>\d+)/views/$',
        TableRowList.as_view(),
        name='table-rows'
    ),
    url(
        r'^tables/(?P<table_id>\d+)/views/(?P<pk>\d+)/$',
        TableRowDetail.as_view(),
        name='table-row-details'
    ),
]
