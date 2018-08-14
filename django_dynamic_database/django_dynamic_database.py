import re
from itertools import chain
from django.db import models
from django.db.models import Aggregate, F, Case, When
from django.core.exceptions import ObjectDoesNotExist

from .models import Table, Column, Row, Cell

import types

"""

We must have database aggregation fonction like SUM, AVG, COUNT ... to on all annotation to generate SQL group_by on only one column (row_id)
Otherwise, Django will do group_by on all fields created in annotations
That why we use custom functions GroupConcat and Concat.

---

For the moment, we use Concat in the generated annotations

"""

class GroupConcat(Aggregate):
    function = 'GROUP_CONCAT'
    template = '%(function)s(%(distinct)s%(expressions)s%(ordering)s%(separator)s)'

    def __init__(self, expression, distinct=False, ordering=None, separator=',', **extra):
        super(GroupConcat, self).__init__(
            expression,
            distinct='DISTINCT ' if distinct else '',
            ordering=' ORDER BY %s' % ordering if ordering is not None else '',
            separator=' SEPARATOR "%s"' % separator,
            output_field=models.CharField(),
            **extra
        )


class Concat(Aggregate):
    function = 'GROUP_CONCAT'
    template = '%(function)s(%(distinct)s%(expressions)s)'

    def __init__(self, expression, distinct=False, **extra):
        super(Concat, self).__init__(
            expression,
            distinct='DISTINCT ' if distinct else '',
            output_field=models.CharField(),
            **extra)

    def as_postgresql(self, compiler, connection):
        # PostgreSQL method
        return self.as_sql(compiler, connection, function='ARRAY_TO_STRING', template="%(function)s(ARRAY_AGG(%(expressions)s), ',')")


# From https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
# Convert Model Name to lower_case_with_underscore
first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()



class DynamicDBModelQuerySet(models.QuerySet):


    ####################################
    # METHODS THAT DO DATABASE QUERIES #
    ####################################
    
    # def all(self, size=None):
    #     return super(DynamicDBModelQuerySet,self).as_manager().get_queryset()
    
    """
    # OK
    def get(self, *args, **kwargs):
        res = super(DynamicDBModelQuerySet,self).as_manager().get_queryset().get(self, *args, **kwargs)
        if isinstance(res, dict):
            res = self._dict_to_object(res) # Converting to object
            # res.__class__ = type(self).__name__
            # res.save = types.MethodType(self._dynamic_object_save, res) # bound save() method to the object
            return res
        else:
            return res
    """
    
    def create(self, defaults=None, **kwargs):
        ids = []

        lookup, params = self._extract_model_params(defaults, **kwargs)
        
        objs = []
        column_names = self._get_columns_name()

        # print(str(column_names))

        table_name = convert(self.model.__name__)
        
        # print(table_name)
        
        table_obj, table_created = Table.objects.get_or_create(name=table_name)
        
        # print(str(table_obj.__dict__))
        
        # Create column for this new table
        if table_created:
            # batch_size = len(column_names) # To use in bulk_create()
            col_set = []
            for colname in column_names:
                col_set.append(Column(table=table_obj, name=colname))
            Column.objects.bulk_create(col_set)
    
    
        if table_obj is not None:
            # Create row to initialize pk
            row_obj = Row.objects.create(table=table_obj)
            if row_obj is not None:
                # print("row_created")
                for attr, val in list(params.items()):
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    # print(str(col_obj.__dict__))
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=str(val)))
                
                Cell.objects.bulk_create(objs)
                # print("cells_bulk_created")
                
                annotations = self._get_custom_annotation()
                # print(str(annotations))
                # print("annotations_created")
                values = self._get_query_values(column_names)
                # print(str(values))
                # print("values_created")
                
                try:
                    obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj).values(**values).order_by()
                    # print(obj)
                    # print("obj_created")
                    res = self._dict_to_object(obj[0])
                    # print(res.std_name)
                    return res
                except ValueError:
                    print(str(self._dict_to_object(obj[0])))


    # OK
    def save(self):
        params = self.__dict__
        params = {k: v() if (callable(v) and v.__name__ != 'save') else v for k, v in params.items()}
        self._save(**params)
    

    def _save(self, **kwargs):
        # Check if provided kwargs is valid
        lookup, params = self._extract_model_params(defaults, **kwargs)

        table_name = convert(self.model.__name__)
        
        table_obj, table_created = Table.objects.get_or_create(name=table_name)
        
        obj_id = kwargs.pop('id', None)
        
        # Check if it is new row
        if obj_id is None:
            row_obj = Row.objects.create(table=table_obj)
        else:
            row_obj = Row.objects.get(pk=obj_id, table=table_obj)
                
        for attr, val in list(params.items()):
            col_obj, col_created = Column.objects.get_or_create(table=table_obj, name=attr)
            cell_obj = Cell.objects.update_or_create(primary_key=row_obj, value_type=col_obj, value=str(val))
            params.pop(attr, None)

    # OK
    def _create_object_from_params(self, lookup, params):
        """
        Try to create an object using passed params. Used by get_or_create()
        and update_or_create().
        """
        try:
            params = {k: v() if callable(v) else v for k, v in params.items()}
            obj = self.create(**params)
            return obj, True
        except IntegrityError as e:
            try:
                return self.get(**lookup), False
            except super(DynamicDBModelQuerySet,self).model.DoesNotExist:
                pass
            raise e


    ##################################################################
    # PUBLIC METHODS THAT ALTER ATTRIBUTES AND RETURN A NEW QUERYSET #
    ##################################################################
    
    
    ###################
    # PRIVATE METHODS #
    ###################

    def _get_columns_name(self):
        # table = Table.objects.get(name=type(self).__name__)
        # return [col.name for col in Column.filter(table=table)]
        cols = list(set(chain.from_iterable(
            (field.name, field.attname) if hasattr(field, 'attname') else (field.name,)
            for field in self.model._meta.get_fields()
            # For complete backwards compatibility, you may want to exclude
            # GenericForeignKey from the results.
            if not (field.name == 'id' or (field.many_to_one and field.related_model is None))
        )))
        return cols


    def _get_custom_annotation(self, table_name=None):

        if table_name is None:
            table_name = convert(self.model.__name__)
        
        # print(table_name)
        # columns = Table.objects.get(name=type(self).__name__).columns.values('id','name')
        # OR
        try:
            columns = Table.objects.get(name=table_name).columns.all().values('id','name')
            return {
                col["name"]:Concat(Case(When(value_type__id=col["id"], then=F('value'))))
                for col in columns
            }
        except ObjectDoesNotExist:
            return None

    def _get_query_values(self, column_names=None):
        # columns = Table.objects.get(name=type(self).__name__).columns.values('id','name')
        # OR
        if column_names is None:
            column_names = self._get_columns_name()

        values = {
            col_name:F(col_name)
            for col_name in column_names
        }
        values["id"]=F('primary_key') # Convert 'primary_key' to 'id'
        
        return values

    
    def _dict_to_object(self, adict):
        """
        Convert a dictionary to a class
        @param :adict Dictionary
        @return :class:Struct
        """
        # return Struct(adict)
        
        res = Struct(adict)
        res.save = types.MethodType(self.save, res) # bound save() method to the object
        res.__class__.__name__ = self.model.__name__
        return res


    def as_manager(cls):
        # Make sure this way of creating managers works.
        manager = DynamicDBModelManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager
    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)


class Struct(object):
    def __init__(self, adict):
        """
        Convert a dictionary to a class
        @param :adict Dictionary
        """
        self.__dict__.update(adict)
        for k, v in adict.items():
            if isinstance(v, dict):
                self.__dict__[k] = Struct(v)


class DynamicDBModelManager(models.Manager):

    def create(self, defaults=None, **kwargs):
        return DynamicDBModelQuerySet(self.model).create(defaults=defaults, **kwargs)


    def save(self):
        return DynamicDBModelQuerySet(self.model).save()


    def get_queryset(self):
    
        table_name = convert(self.model.__name__)
        
        # print(table_name)

        annotations = DynamicDBModelQuerySet(self.model)._get_custom_annotation(table_name)

        if annotations is None:
            return models.QuerySet(self.model).none()
    
        column_names = DynamicDBModelQuerySet(self.model)._get_columns_name()
        
        values = DynamicDBModelQuerySet(self.model)._get_query_values(column_names)

        return Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()


    def get(self, *args, **kwargs):
    
        res = self.get_queryset().get(*args, **kwargs)
        # print(str(res))
        if isinstance(res, dict):
            res = DynamicDBModelQuerySet(self.model)._dict_to_object(res) # Converting to object
            res.__class__.__name__ = self.model.__name__
            res.save = types.MethodType(self.save, res) # bound save() method to the object
            return res
        else:
            return res


    # def get_or_create(self, defaults=None, **kwargs):
    #    return DynamicDBModelQuerySet(self.model).get_or_create(defaults=defaults, **kwargs)

    def get_or_create(self, defaults=None, **kwargs):
        
        lookup, params = DynamicDBModelQuerySet(self.model)._extract_model_params(defaults, **kwargs)
        
        try:
            res = self.get(**lookup)
            if isinstance(res, dict):
                res = DynamicDBModelQuerySet(self.model)._dict_to_object(res) # Converting to object
                res.__class__.__name__ = self.model.__name__
                res.save = types.MethodType(DynamicDBModelQuerySet(self.model).save, res) # bound save() method
                return res, False
            else:
                return res, False
        except:
            return DynamicDBModelQuerySet(self.model)._create_object_from_params(lookup, params)


    def update_or_create(self, defaults=None, **kwargs):
        defaults = defaults or {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        try:
            obj = self.get_queryset().get(**lookup)
            if isinstance(obj, dict):
                obj = DynamicDBModelQuerySet(self.model)._dict_to_object(obj) # Converting to object
                obj.__class__.__name__ = self.model.__name__
                obj.save = types.MethodType(DynamicDBModelQuerySet(self.model).save, obj) # bound save() method
            for k, v in defaults.items():
                setattr(obj, k, v() if callable(v) else v)
                obj.save()
            return obj, False
        except self.model.DoesNotExist:
            obj, created = DynamicDBModelQuerySet(self.model)._create_object_from_params(lookup, params)
            if created:
                return obj, created




    """
    Generating custom initial queryset by pivoting datatable cell.
    ----
    SELECT row_id AS id,
    (CASE WHEN column_name = 'xxx' THEN value END) AS column_name,
    ...
    AGGREGATION(CASE WHEN column_name = 'xxx' THEN value END) AS column_name,
    FROM "Cell"
    GROUP BY id;
    ""
    def get_queryset(self):
    
        # Generate models columns annotation
        annotations = self._get_custom_annotation()
        # Generate final models columns values
        values = self._get_query_values()

        ""
        return DynamicDBModelQuerySet(self.model, using=self._db)
            .filter(entity=type(self).__name__)  # Important!
            .values('primary_key')  # Important, # values + annotate => GROUP BY row_id
            .annotate(**annotations) # Annotate with columns_name
            .order_by() # Important
        ""
        table_name = convert(type(self).__name__)

        return Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()
    """

class DynamicDBModel(models.Model):
    
    objects = DynamicDBModelManager()
    # objects = DynamicDBModelQuerySet.as_manager()
    
    class Meta:
        abstract = True

    # OK
    def save(self):
        params = self.__dict__
        params.pop('_state', None)
        params = {k: v() if (callable(v) and v.__name__ != 'save') else v for k, v in params.items()}
        self._save(**params)
    

    def _save(self, **kwargs):
        # Check if provided kwargs is valid
        defaults = {}
        lookup, params = DynamicDBModelQuerySet(self)._extract_model_params(defaults, **kwargs)
        
        print(str(params))

        table_name = convert(self.__class__.__name__)
        
        table_obj, table_created = Table.objects.get_or_create(name=table_name)
        
        obj_id = kwargs.pop('id', None)
        
        # Check if it is new row
        if obj_id is None:
            row_obj = Row.objects.create(table=table_obj)
        else:
            row_obj = Row.objects.get(pk=obj_id, table=table_obj)
                
        for attr, val in list(params.items()):
            col_obj, col_created = Column.objects.get_or_create(table=table_obj, name=attr)
            cell_obj = Cell.objects.update_or_create(primary_key=row_obj, value_type=col_obj, value=str(val))

"""
    def get_queryset(self):

        table_name = convert(self.model.__name__)

        annotations = DynamicDBModelQuerySet(self.model)._get_custom_annotation(table_name)
        
        if annotations is None:
            return DynamicDBModelQuerySet(self.model).none()
        
        column_names = DynamicDBModelQuerySet(self.model)._get_columns_name()
        
        values = DynamicDBModelQuerySet(self.model)._get_query_values(column_names)

        return Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()




"""

