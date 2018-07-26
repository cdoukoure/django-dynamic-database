import re
from django.core import exceptions
from django.apps import apps
from django.db import models
from django.db.models import Aggregate, Avg, Q, F, sql

import types

class Table(models.Model):

    name = models.CharField(max_length=30)

    def __str__(self):
        return self.name
    
    
class Column(models.Model):

    name = models.CharField(max_length=100)
    table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='columns')
    
    def __str__(self):
        return self.name


class Row(models.Model):

    table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='rows')
    cells = models.ManyToManyField(Column, through='Cell', related_name='+')

    def __str__(self):
        return self.name

class Cell(models.Model):

    primary_key = models.ForeignKey('Row', on_delete=models.CASCADE)
    value_type = models.ForeignKey('Column', on_delete=models.CASCADE)
    value = models.CharField(max_length=500)


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



# From https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
# Convert Model Name to lower_case_with_underscore
first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')
def convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()



class DynamicDBModelQuerySet(models.QuerySet):

    ################################################################
    #  Used in QuerySet.get() to convert dict into class instance  #
    ################################################################

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


    def _dict_to_object(adict):
        """
        Convert a dictionary to a class
        @param :adict Dictionary
        @return :class:Struct
        """
        # return Struct(adict)
        
        res = Struct(adict)
        res.save = types.MethodType(self._dynamic_object_save, res) # bound save() method to the object
        res.__class__.__name__ = type(self).__name__
        return res


    ####################################
    # METHODS THAT DO DATABASE QUERIES #
    ####################################

    # OK
    def get(self, *args, **kwargs):
        # returning dict
        res = super(DynamiDBModelQuerySet,self).get(self, *args, **kwargs)
        
        if isinstance(res, dict):
            res = self._dict_to_object(res) # Converting to object
            # res.__class__ = type(self).__name__
            # res.save = types.MethodType(self._dynamic_object_save, res) # bound save() method to the object
            return res
        else:
            return res
        
    # OK
    def create(self, defaults=None, **kwargs):
        ids = []
        # table, created = Table.objects.get_or_create(name=type(self).__name__)
        # cols_name = [col.name for col in Column.filter(table=table)]

        # lookup, params = self._extract_model_params(defaults, **kwargs)
        
        objs = []
        
        table_name = convert(type(self).__name__)
        
        table_obj, table_created = Table.objects.get_or_create(name=table_name)
        
        # Create column for this new table
        if table_created:
            column_names = self._get_columns_name()
            # batch_size = len(column_names) # To use in bulk_create()
            col_set = []
            for colname in column_names:
                col_set.append(Column(table=table_obj, name=colname))
            Column.objects.bulk_create(col_set)
        
        if table_obj is not None:
            # Create row to initialize pk
            row_obj = Row.objects.create(table=table_obj)
            if row_obj is not None:
                for attr, val in kwargs.iteritems():
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=val))

                Cell.objects.bulk_create(objs)
                
                annotations = self._get_custom_annotation()
                
                obj = Cell.objects.values('primary_key').annotate(**annotations).filter(primary_key=row_obj)
                
                return self._dict_to_object(obj._result_cache[0])

    # OK
    def get_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, creating one if necessary.
        Return a tuple of (object, created), where created is a boolean
        specifying whether an object was created.
        """
        lookup, params = self._extract_model_params(defaults, **kwargs)

        try:
            return self.get(**lookup), False
        except self.model.DoesNotExist:
            return self._create_object_from_params(lookup, params)


    # OK
    def _dynamic_object_save(self):
        params = self.__dict__
        return self.update_or_create(**params)
    

    def update_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, updating one with defaults
        if it exists, otherwise create a new one.
        Return a tuple (object, created), where created is a boolean
        specifying whether an object was created.
        """
        defaults = defaults or {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        try:
            # obj = Cell.objects.select_for_update().get(**lookup)
            obj = self.get(**lookup)
        except super(DynamiDBModelQuerySet,self).model.DoesNotExist:
            obj, created = self._create_object_from_params(lookup, params)
            if created:
                return obj, created

        for attr, val in defaults.items():
            for k, v in defaults.items():
                setattr(obj, k, v() if callable(v) else v)
            obj.save()
        return obj, False


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
            except super(DynamiDBModelQuerySet,self).model.DoesNotExist:
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
            for field in MyModel._meta.get_fields()
            # For complete backwards compatibility, you may want to exclude
            # GenericForeignKey from the results.
            if not (field.many_to_one and field.related_model is None)
        )))
        return cols


    def _get_custom_annotation(self):
        table_name = convert(type(self).__name__)
        # columns = Table.objects.get(name=type(self).__name__).columns.values('id','name')
        # OR
        columns = Table.objects.get(name=table_name).columns.all().values('id','name')

        return {
            column_name:Concat(Case(When(column_id=col_id, then=F('value'))))
            for col_id, col_name in columns
        }

    def _get_values_columns(self):
        # columns = Table.objects.get(name=type(self).__name__).columns.values('id','name')
        # OR
        column_names = self._get_columns_name()

        values = {
            col_name:F(col_name)
            for col_name in column_names
        }
        values[id]=F('primary_key') # Convert 'primary_key' to 'id'
        
        return values

    
    def as_manager(cls):
        # Make sure this way of creating managers works.
        manager = DynamicDBModelManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager
    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)



class DynamicDBModelManager(models.Manager):
    """
    Base class for a model manager.
    """
    #: The queryset class to use.
    queryset_class = DynamicDBModelQuerySet

 
    """
    Generating custom initial queryset by pivoting datatable cell.
    ----
    SELECT row_id AS id,
    (CASE WHEN column_name = 'xxx' THEN value END) AS column_name,
    ...
    AGGREGATION(CASE WHEN column_name = 'xxx' THEN value END) AS column_name,
    FROM "Cell"
    GROUP BY id;
    """
    def get_queryset(self):
    
        # Generate models columns annotation
        annotations = super(DynamicDBModelQuerySet,self)._get_custom_annotation()
        # Generate final models columns values
        values = super(DynamicDBModelQuerySet,self)._get_values_columns()

        """"
        return DynamiDBModelQuerySet(self.model, using=self._db)
            .filter(entity=type(self).__name__)  # Important!
            .values('primary_key')  # Important, # values + annotate => GROUP BY row_id
            .annotate(**annotations) # Annotate with columns_name
            .order_by() # Important
        """"
        table_name = convert(type(self).__name__)

        return Cell.objects.filter(primary_key__table__name=table_name)  # Important!
            .values('primary_key')  # Important, # values + annotate => GROUP BY row_id
            .annotate(**annotations) # Annotate with columns_name
            .values(**values)  # Convert 'primary_key' to 'id'
            .order_by() # Important


class DynamicDBModel(models.Model):
    
    objects = DynamicDBModelManager()

    class Meta:
        abstract = True


