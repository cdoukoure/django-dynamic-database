import json
from django.core import serializers
from django.http import HttpResponse, JsonResponse, Http404
from rest_framework import permissions, status, views
# from rest_framework.decorators import api_view
from rest_framework.views import APIView
from rest_framework.response import Response


from .models import Table, Column, Row, Cell

from .serializers import RowSerializer, ColumnSerializer, TableSerializer, CellSerializer

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



# Examples of views for dynamic Entity

class EntityList(APIView):


    def get_queryset(self, table_name):
        annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name)
        if annotations is None:
            qs = models.QuerySet(self.model).none()
        column_names = [k for k in annotations]
        values = DynamicDBModelQuerySet(self)._get_query_values(column_names)
        return Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()
    
    def create(self, validated_data, table_obj):
        defaults = None
        objs = []
        if table_obj:
            # Create row to initialize pk
            row_obj = Row.objects.create(table=table_obj)
            if row_obj is not None:
                annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name=table_obj.name)
                column_names = [str(k) for k in annotations]
                defaults = {str(k):str(k) for k in column_names}
                for attr, val in list(validated_data.items()):
                    defaults.pop(attr)
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=str(val)))
                for attr in defaults:
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=null))
                Cell.objects.bulk_create(objs)
                values = DynamicDBModelQuerySet(self)._get_query_values(column_names)
            try:
                obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj).values(**values).order_by()
                return obj
            except ValueError:
                raise serializers.ValidationError('Please check yours fields values.')

    def update(self, validated_data, table_obj):
        
        row_id = validated_data.pop('id')
            
        for attr, val in validated_data.items():
            Cell.objects.filter(primary_key__id=row_id, value_type=Column.objects.get(table=table_obj, name=attr)).update(value=val)
            
        try:
            obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj).values(**values).order_by()
            return obj
        except ValueError:
            raise serializers.ValidationError('Please check yours fields values.')


    def get(self, request, table_id):
        table = Table.objects.get(pk=table_id)
        qset = self.get_queryset(table.name)
        qs = [ obj for obj in qset ]
        return HttpResponse(json.dumps({"data": qs}), content_type='application/json')


    def post(self, request, table_id):
        print(request.data)
        try:
            table_obj = Table.objects.get(pk=table_id)
            row_id = request.data.get('id', None)
            if row_id is not None:
                return Response(e, status=status.HTTP_400_BAD_REQUEST)
            else:
                qs = self.create(request.data, table_obj)
                return HttpResponse(json.dumps({"data": [qs[0]]}), content_type='application/json', status=status.HTTP_201_CREATED)
        except ValueError as e:
            return Response(e, status=status.HTTP_400_BAD_REQUEST)
        

class EntityDetail(APIView):

    def get(self, request, table_id, pk):
        return Response(status=status.HTTP_204_NO_CONTENT)

    def put(self, request, table_id, pk):
        return Response(status=status.HTTP_204_NO_CONTENT)

    def delete(self, request, table_id, pk):
        return Response(status=status.HTTP_204_NO_CONTENT)
