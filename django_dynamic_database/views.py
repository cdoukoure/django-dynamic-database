import json
from django.core import serializers
from django.http import HttpResponse, JsonResponse, Http404
from rest_framework import permissions, status, views
# from rest_framework.decorators import api_view
from rest_framework.views import APIView
from rest_framework.response import Response


from .models import Table, Column, Row, Cell

from .serializers import RowSerializer, ColumnSerializer, TableRowSerializer, TableSerializer, CellSerializer

from .django_dynamic_database import convert, DynamicDBModelQuerySet


class TableList(APIView):

    def get(self, request, format=None):
        tables = Table.objects.all()
        serializer = TableSerializer(tables, many=True)
        return Response(serializer.data)

    def post(self, request, format=None):
        serializer = TableSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        

class TableDetail(APIView):
    
    def get_object(self, pk):
        try:
            return Table.objects.get(pk=pk)
        except Table.DoesNotExist:
            raise Http404

    def get(self, request, pk, format=None):
        table = self.get_object(pk)
        serializer = TableSerializer(table)
        return Response(serializer.data)

    def put(self, request, pk, format=None):
        table = self.get_object(pk)
        serializer = TableSerializer(table, data=request.data)
        # raise ValueError(table.columns.first().id)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk, format=None):
        table = self.get_object(pk)
        table.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class TableRowList(APIView):


    def get_queryset(self, table_name):
        annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name)
        if annotations is None:
            qs = models.QuerySet(self.model).none()
        column_names = [k for k in annotations]
        values = DynamicDBModelQuerySet(self)._get_query_values(column_names)
        return Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()
    
    """
    def get_serializer_class(self, table_name):
        annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name)
        if annotations is None:
            qs = models.QuerySet(self.model).none()
        TableRowSerializer.Meta.model = self.kwargs.get('model')
        TableRowSerializer.Meta.fields = [k for k in annotations]
        return TableRowSerializer
    """
    
    def get(self, request, table_id):
        # table_id = request.data['id']
        table = Table.objects.get(pk=table_id)
        qset = self.get_queryset(table.name)
        qs = [ obj for obj in qset ]
        return HttpResponse(json.dumps({"data": qs}), content_type='application/json')


    def post(self, request, table_id):
        # serializer_class = self.get_serializer_class(table.name)
        # table = Table.objects.get(pk=table_id)
        # if serializer.is_valid():
        print(request.data)
        raising = None
        try:
            table_obj = Table.objects.get(pk=table_id)
            annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name=table_obj.name)
            TableRowSerializer.Meta.fields = (k for k in annotations)
            serializer = TableRowSerializer(data=request.data)
            if serializer.is_valid():
                raising = serializer.save()
            # return Response(serializer.data, status=status.HTTP_201_CREATED)
            return HttpResponse(json.dumps({"data": serializer.data}), content_type='application/json', status=status.HTTP_201_CREATED)
        except:
            return Response(serializer.data, status=status.HTTP_400_BAD_REQUEST)
        

# NOK
class TableRowDetail(APIView):
    
    def get_object(self, table_id, pk):
        try:
            return Table.objects.get(pk=pk)
        except Table.DoesNotExist:
            raise Http404

    def get(self, request, table_id, pk, format=None):
        table = self.get_object(table_id, pk)
        serializer = TableSerializer(table)
        return Response(serializer.data)

    def put(self, request, table_id, pk, format=None):
        table = self.get_object(table_id, pk)
        serializer = TableSerializer(table, data=request.data)
        # raise ValueError(table.columns.first().id)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, table_id, pk, format=None):
        table = self.get_object(table_id, pk)
        table.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)



