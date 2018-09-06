import itertools
from rest_framework import serializers

from rest_framework.exceptions import ErrorDetail, ValidationError


from .django_dynamic_database import convert, DynamicDBModelQuerySet
from .models import Table, Column, Row, Cell



class RowSerializer(serializers.ModelSerializer):
    id = serializers.ModelField(model_field=Row._meta.get_field('id'), required=False)
    class Meta:
        model = Row
        fields = ('id')


class ColumnSerializer(serializers.ModelSerializer):
    id = serializers.ModelField(model_field=Column._meta.get_field('id'), required=False)
    class Meta:
        model = Column
        fields = ('id', 'name')


class TableRowSerializer(serializers.ModelSerializer):
    
    class Meta:
        model = None

    # def __call__(self, value):
    #     if value % self.base != 0:
    #         message = 'This field must be a multiple of %d.' % self.base
    #         raise serializers.ValidationError(message)
    
    def is_valid(self, raise_exception=False):
        # This implementation is the same as the default,
        # except that we use lists, rather than dicts, as the empty case.
        assert hasattr(self, 'initial_data'), (
            'Cannot call `.is_valid()` as no `data=` keyword argument was '
            'passed when instantiating the serializer instance.'
        )

        if not hasattr(self, '_validated_data'):
            try:
                self._validated_data = self.run_validation(self.initial_data)
            except ValidationError as exc:
                self._validated_data = []
                self._errors = exc.detail
            else:
                self._errors = []

        if self._errors and raise_exception:
            raise ValidationError(self.errors)

        return not bool(self._errors)


    def run_validation(self, data=empty):
        """
        We override the default `run_validation`, because the validation
        performed by validators and the `.validate()` method should
        be coerced into an error dictionary with a 'non_fields_error' key.
        """
        (is_empty_value, data) = self.validate_empty_values(data)
        if is_empty_value:
            return data

        value = self.to_internal_value(data) # [{},{}]
        print(' to_internal_value ')
        print(value)
        print(' to_internal_value ')
        try:
            # self.run_validators(value)
            value = self.validate(value)
            assert value is not None, '.validate() should return the validated data'
        except (ValidationError, DjangoValidationError) as exc:
            raise ValidationError(detail=as_serializer_error(exc))

        return value

    
    def validate(self, data):
        """
        Check that the start is before the stop.
        """
        data_id = data['table_id']
        del data['table_id']
        table_obj = Table.objects.get(pk=data_id)
        annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name=table_obj.name)
        column_names = [k for k in annotations]

        for attr, v in data:
            if attr != 'id' and attr not in column_names:
                raise serializers.ValidationError("finish must occur after start")
        return data


    def create(self, validated_data):
        # validated_data = {table_id:value, others rows fields}
        defaults = None
        objs = []
        table_id = validated_data.pop('table_id')
        table_obj = Table.objects.get(pk=table_id)
        
        if table_obj:
            # Create row to initialize pk
            row_obj = Row.objects.create(table=table_obj)
            
            if row_obj is not None:
                annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name=table_obj.name)
                
                column_names = [k for k, v in annotations]
                
                defaults = column_names
                
                for attr, val in list(validated_data.items()):
                    defaults.pop(attr)
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=str(val)))
                
                for attr in defaults:
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=null))
                
                Cell.objects.bulk_create(objs)
                
                # Initialize annotations and values to return query_set from pivot
                values = DynamicDBModelQuerySet(self)._get_query_values(column_names)
                
            try:
                obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj).values(**values).order_by()
                # Converting value_query_set to object
                res = DynamicDBModelQuerySet(self)._dict_to_object(obj[0])
                return res
            except ValueError:
                # raise("Please check yours fields values")
                raise serializers.ValidationError('Please check yours fields values.')

    def update(self, instance, validated_data):
        
        row_id = validated_data.pop('id')
            
        table_id = validated_data.pop('table_id')
        table_obj = Table.objects.get(pk=table_id)
        
        for field in validated_data:
            instance.__setattr__(field, validated_data.get(field))
        
        for attr, val in validated_data.items():
            qs = Cell.objects.filter(primary_key__id=row_id, value_type=Column.objects.get(table=table_obj, name=attr)).update(value=val)
            
            return instance


class TableSerializer(serializers.ModelSerializer):
    id = serializers.ModelField(model_field=Table._meta.get_field('id'), required=False)
    columns = ColumnSerializer(many=True)
    # rows = RowSerializer(many=True)
    
    class Meta:
        model = Table
        # fields = ('id','name', 'description','columns','rows')
        fields = ('id','name','columns')
    
    def create(self, validated_data):
        columns_data = validated_data.pop('columns')
        validated_data.set('name', convert(validated_data.get('name')))
        # rows_data = validated_data.pop('rows')
        table = Table.objects.create(**validated_data)
        for col_data in columns_data:
            Column.objects.create(table=table, **col_data)
        # for row_data in rows_data:
        #    Row.objects.create(table=table, **row_data)
        return table
    
    def update(self, instance, validated_data):
        
        not_to_delete = [] # Rows and Columns Not to delete
        
        columns_data = validated_data.pop('columns')
        columns = instance.columns.all()
        
        instance.id = validated_data.get('id', instance.id)
        instance.name = validated_data.get('name', instance.name)
        instance.save()
        
        for col_data in columns_data:
            if columns:
                col = [x for x in columns if x.id == col_data.get('id')]
                if len(col) > 0:
                    column = Column.objects.get(id=col[0].id)
                    for field in col_data:
                        column.__setattr__(field, col_data.get(field))
                        column.table = instance
                    column.save()
                    not_to_delete.append(col[0].id)
                else:
                    Column.objects.create(table = instance, **col_data)
            else:
                Column.objects.create(table = instance, **col_data)
        # delete old columns
        if columns:
            for col in columns:
                if col.id not in not_to_delete:
                    Column.objects.filter(id=col.id).delete()
        return instance


class CellSerializer(serializers.ModelSerializer):
    
    id = serializers.ModelField(model_field=Cell._meta.get_field('id'), required=False)
    primary_key = RowSerializer()
    value_type = ColumnSerializer()
    
    class Meta:
        model = Cell
        fields = ('id', 'primary_key', 'value_type', 'value')


class DataRowsSerializer(serializers.ModelSerializer):
    
    
    def get_rows(self, table_name):
        annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name)
        if annotations is None:
            qs = models.QuerySet(self.model).none()
        
        column_names = [k for k, v in annotations]
        values = DynamicDBModelQuerySet(self)._get_query_values(column_names)
        
        qs = Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()
        
        return JsonResponse(serializers.serialize("json", qs))

    def create(self, validated_data):
        # validated_data = {table_id, others rows fields}
        defaults = None
        objs = []
        table_id = validated_data.pop('table_id')
        table_obj = Table.objects.get(pk=table_id)
        
        if table_obj:
            # Create row to initialize pk
            row_obj = Row.objects.create(table=table_obj)
            
            if row_obj is not None:
                annotations = DynamicDBModelQuerySet(self)._get_custom_annotation(table_name=table_obj.name)
                
                column_names = [k for k, v in annotations]
                
                defaults = column_names
                
                for attr, val in list(validated_data.items()):
                    defaults.pop(attr)
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=str(val)))
                
                for attr in defaults:
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=null))
                
                Cell.objects.bulk_create(objs)
                
                # Initialize annotations and values to return query_set from pivot
                values = DynamicDBModelQuerySet(self)._get_query_values(column_names)
                
                try:
                    obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj).values(**values).order_by()
                    # Converting value_query_set to object
                    res = DynamicDBModelQuerySet(self)._dict_to_object(obj[0])
                    return res
                except ValueError:
                    # raise("Please check yours fields values")
                    raise serializers.ValidationError('Please check yours fields values.')

    def update(self, instance, validated_data):
        
        row_id = validated_data.get('id')
            
        table_id = validated_data.get('table_id')
        table_obj = Table.objects.get(pk=table_id)
        
        for field in validated_data:
            instance.__setattr__(field, validated_data.get(field))
    
        for attr, val in validated_data.items():
            qs = Cell.objects.filter(primary_key__table=table_obj, primary_key__id=row_id, value_type=Column.objects.get(table=table_obj, name=attr)).update(value=val)
            
        return instance



