import re
from itertools import chain
from django.db import models
from django.db.models import Aggregate, Sum, Q, F, Case, When
from django.core.exceptions import ObjectDoesNotExist, FieldError

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


class Sum(Aggregate):
    function = 'SUM'
    name = 'Sum'

    def as_oracle(self, compiler, connection):
        if self.output_field.get_internal_type() == 'DurationField':
            expression = self.get_source_expressions()[0]
            from django.db.backends.oracle.functions import IntervalToSeconds, SecondsToInterval
            return compiler.compile(
                SecondsToInterval(Sum(IntervalToSeconds(expression)))
            )
        return super().as_sql(compiler, connection)

    def as_postgresql(self, compiler, connection):
        # PostgreSQL method
        sql, params = self.as_sql(compiler, connection, template='%(function)s(%(expressions))')
        print(sql)
        print(params)
        return sql, params


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
    
    def get_queryset(self, ids=None):
        
        table_name = convert(self.model.__name__)
        
        annotations = self._get_custom_annotation(table_name)
        
        if annotations is None:
            return models.QuerySet(self.model).none()
    
        column_names = self._get_columns_name()
        
        values = self._get_query_values(column_names)

        if ids is not None:
            object_set = Cell.objects.filter(primary_key__table__name=table_name, primary_key__id__in=ids).values('primary_key').annotate(**annotations).values(**values).order_by()
        else:
            object_set = Cell.objects.filter(primary_key__table__name=table_name).values('primary_key').annotate(**annotations).values(**values).order_by()
        
        object_set._fields = None
        
        object_set.update = types.MethodType(self.update, object_set) # bound delete() method
        object_set.delete = types.MethodType(self.delete, object_set) # bound delete() method
        return object_set
    

    def get(self, *args, **kwargs):
        res = self.get_queryset().get(*args, **kwargs)
        # print(str(res))
        if isinstance(res, dict) and res != {}:
            res = self._dict_to_object(res) # Converting to object
            return res
    

    def create(self, **kwargs):
        defaults=None
        lookup, params = self._extract_model_params(defaults, **kwargs)
        objs = []
        ids = []
        column_names = self._get_columns_name()
        table_name = convert(self.model.__name__)
        table_obj, table_created = Table.objects.get_or_create(name=table_name)
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
                for attr, val in list(params.items()):
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=str(val)))
                Cell.objects.bulk_create(objs)
                # Initialize annotations and values to return query_set from pivot
                annotations = self._get_custom_annotation()
                values = self._get_query_values(column_names)
                try:
                    obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj).values(**values).order_by()
                    # Converting value_query_set to object
                    res = self._dict_to_object(obj[0])
                    return res
                except ValueError:
                    pass


    def bulk_create(self, objs, batch_size=None):
        pass


    def get_or_create(self, defaults=None, **kwargs):
        lookup, params = self._extract_model_params(defaults, **kwargs)
        try:
            res = self.get(**lookup)
            if isinstance(res, dict):
                res = self._dict_to_object(res) # Converting to object
                return res, False
            else:
                return res, False
        except:
            return self._create_object_from_params(lookup, params)


    def update_or_create(self, defaults=None, **kwargs):
        defaults = defaults or {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        try:
            obj = self.get_queryset().get(**lookup)
            if isinstance(obj, dict):
                obj = self._dict_to_object(obj) # Converting to object
            for k, v in defaults.items():
                setattr(obj, k, v() if callable(v) else v)
            obj.save()
            return obj, False
        except self.model.DoesNotExist:
            obj, created = self._create_object_from_params(lookup, params)
            if created:
                return obj, created


    def save(self, obj):
        try:
            params = obj.__dict__
            params.pop('save', None)
            params.pop('delete', None)
            pars = {k: v() if callable(v) else v for k, v in params.items()}
            self._save(**pars)
        except ValueError as e:
            raise(e)


    def _save(self, **kwargs):
        
        defaults = {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        
        table_name = convert(self.model.__name__)
        column_names = self._get_columns_name()
        
        obj_id = kwargs.pop('id', None)
        
        table_obj = Table.objects.get(name=table_name)
        
        # Check if it is new row
        if obj_id is None:
            row_obj = Row.objects.create(table=table_obj)
        else:
            row_obj = Row.objects.get(pk=obj_id, table=table_obj)
        
        # To-Do: Improve with bulk_update
        for attr, val in list(params.items()):
            col_obj, col_created = Column.objects.get_or_create(table=table_obj, name=attr)
            cell_obj, cell_created = Cell.objects.get_or_create(primary_key=row_obj, value_type=col_obj)
            cell_obj.value = str(val)
            cell_obj.save()


    def delete(self, queryset_or_obj):

        assert self.query.can_filter(), \
            "Cannot use 'limit' or 'offset' with delete."

        if hasattr(queryset_or_obj, '_fields') and queryset_or_obj._fields is not None:
            raise TypeError("Cannot call delete() after .values() or .values_list()")

        try:
            if queryset_or_obj.__class__.__name__ == "QuerySet":
                row_ids = []
                for c in queryset_or_obj:
                    c = self._dict_to_object(c)
                    row_ids.append(c.id)
                row_ids = list(set(row_ids))
                num = len(row_ids)
                cell_objs = Cell.objects.filter(primary_key__id__in=row_ids)
                if cell_objs.exists():
                    # Raw delete with the convenience of using Django QuerySet
                   cell_objs._raw_delete(cell_objs.db)
                   return (num, {'webapp.Entry': num})
                raise TypeError("System error. QuerySet deletion failed.")
            else:
                params = queryset_or_obj.__dict__
                params.pop('save', None)
                params.pop('delete', None)
                pars = {k: v() if callable(v) else v for k, v in params.items()}
                return self._delete_object(**params)
        except ValueError as e:
            raise(e)


    def _delete_object(self, **kwargs):
        
        defaults = {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        
        table_name = convert(self.model.__name__)
        table_obj = Table.objects.get(name=table_name)
        
        cell_set = []
        # To-Do: Improve with bulk_update
        for attr, val in list(params.items()):
            col_obj, col_created = Column.objects.get_or_create(table=table_obj, name=attr)
            cell_objs = Cell.objects.filter(value_type=col_obj, value=val)
            for c in cell_objs:
                cell_set.append(c.primary_key.pk)
        cell_set = list(set(cell_set))
        num = len(cell_set)
        cell_objs = Cell.objects.filter(primary_key__id__in=cell_set)
        if cell_objs.exists():
            # Raw delete with the convenience of using Django QuerySet
            cell_objs._raw_delete(cell_objs.db)
            return num, {__package__ + '.' + self.model.__name__: num}
        else:
            return 0, {__package__ + '.' + self.model.__name__: 0}


    def update(self, queryset, **kwargs):
        assert queryset.query.can_filter(), \
            "Cannot update a query once a slice has been taken."
        table_name = convert(self.model.__name__)
        table_obj = Table.objects.get(name=table_name)
        row_ids = []
        for c in queryset:
            c = self._dict_to_object(c)
            row_ids.append(c.id)
        row_ids = list(set(row_ids))
        for attr, val in kwargs.items():
            qs = Cell.objects.filter(primary_key__id__in=row_ids, value_type=Column.objects.get(table=table_obj, name=attr)).update(value=val)
        return self.get_queryset(ids=row_ids)


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
    
    
    ##########################
    # CUSTOM PRIVATE METHODS #
    ##########################

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
        @return :class:DictObj
        """
        res = DictObj(adict)
        res.save = types.MethodType(self.save, res) # bound save() method to the object
        res.delete = types.MethodType(self.delete, res) # bound save() method to the object
        if self.model is not None:
            res.__class__.__name__ = self.model.__name__
        return res


    def as_manager(cls):
        # Make sure this way of creating managers works.
        manager = DynamicDBModelManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager
    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)


class DictObj(object):
    def __init__(self, adict):
        # Convert a dictionary to a class @param :adict Dictionary
        self.__dict__.update(adict)
        for k, v in adict.items():
            if isinstance(v, dict):
                self.__dict__[k] = DictObj(v)


class ObjDict(dict):
    def __getattribute__(self, item):
        try:
            if isinstance(self[item], str):
                self[item] = import_string(self[item])
            value = self[item]
        except KeyError:
            value = super(ObjDict, self).__getattribute__(item)

        return value


class DynamicDBModelManager(models.Manager):

    ####################################
    # METHODS THAT DO DATABASE QUERIES #
    ####################################
    
    def get_queryset(self, ids=None):
        return DynamicDBModelQuerySet(self.model).get_queryset(ids)


    def get(self, *args, **kwargs):
        return DynamicDBModelQuerySet(self.model).get(*args, **kwargs)


    def filter(self, *args, **kwargs):
        res = self.get_queryset().filter(*args, **kwargs)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def exclude(self, *args, **kwargs):
        res = self.get_queryset().exclude(*args, **kwargs)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def union(self, *other_qs, all=False):
        res = self.get_queryset().union(*other_qs, all)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def intersection(self, *other_qs):
        res = self.get_queryset().intersection(*other_qs)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def difference(self, *other_qs):
        res = self.get_queryset().difference(*other_qs)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def annotate(self, *args, **kwargs):
        res = self.get_queryset().annotate(*args, **kwargs)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def order_by(self, *field_names):
        res = self.get_queryset().order_by(*field_names)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def distinct(self, *field_names):
        res = self.get_queryset().distinct(*field_names)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def reverse(self):
        res = self.get_queryset().reverse()
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def defer(self, *fields):
        res = self.get_queryset().defer(*fields)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def only(self, *fields):
        res = self.get_queryset().only(*fields)
        res.update = types.MethodType(self.update, res) # bound custom update() method
        res.delete = types.MethodType(self.delete, res) # bound custom delete() method
        return res


    def create(self, **kwargs):
        return DynamicDBModelQuerySet(self.model).create(**kwargs)


    def bulk_create(self, objs, batch_size=None):
        pass


    def get_or_create(self, defaults=None, **kwargs):
        return DynamicDBModelQuerySet(self.model).get_or_create(defaults, **kwargs)


    def update_or_create(self, defaults=None, **kwargs):
        return DynamicDBModelQuerySet(self.model).update_or_create(defaults, **kwargs)


    def delete(self, queryset_or_obj):
        return DynamicDBModelQuerySet(self.model).delete(queryset_or_obj)


    def update(self, queryset, **kwargs):
        return DynamicDBModelQuerySet(self.model).update(queryset, **kwargs)




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


    def save(self):
        try:
            params = self.__dict__
            # print(params)
            params.pop('_state', None)
            params.pop('save', None)
            params.pop('delete', None)
            # print(params)
            params = {k: v() if callable(v) else v for k, v in params.items()}
            self._save(**params)
        except TypeError:
            pass


    def _save(self, **kwargs):
        # Check if provided kwargs is valid
        defaults = {}

        lookup, params = DynamicDBModelQuerySet(self)._extract_model_params(defaults, **kwargs)
        
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
            # if attr == 'weight':
            #     print(val)
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


language: python
python:
  - "2.7"
  - "3.4"
  - "3.5"
  - "3.6"
services:
  - mysql
  - postgresql
env:
  - DJANGO=1.11 DB=sqlite
  - DJANGO=1.11 DB=mysql
  - DJANGO=1.11 DB=postgres

before_script:
  - mysql -e 'create database dynamic_db;'
  - psql -c 'create database dynamic_db;' -U postgres
install:
  - pip install pip --upgrade
  - if [ "$DB" == "mysql" ]; then pip install mysqlclient; fi
  - if [ "$DB" == "postgres" ]; then pip install psycopg2-binary; fi
  - pip install -q Django==$DJANGO
script:
  - python runtests.py --settings=django_dynamic_database.tests.test_"$DB"_settings


"""

