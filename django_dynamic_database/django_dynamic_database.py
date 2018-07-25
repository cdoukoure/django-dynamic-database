from django.core import exceptions
from django.apps import apps
from django.db import models
from django.db.models import Aggregate, Avg, Q, F, sql
try:
    from threading import local
except ImportError:
    from django.utils._threading_local import local

from collections import OrderedDict
import pdb

_thread_locals = local()

"""

We must have database aggregate fonction like SUM, AVG, COUNT ... to generate SQL group_by on only one column (row_id)
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


#var=Concat(Case(When(column__name='var', then="value"), default='value',), Value(''), output_field=CharField())

"""
sqlite & MySql: GROUP_CONCAT(column_name) === PostgreSQL: array_to_string(array_agg(column_name), ',') === Oracle: LISTAGG(column_name, ',')

SELECT row_id,
(CASE WHEN column_name = 'nom' THEN value END) AS nom,
(CASE WHEN column_name = 'genre' THEN value END) AS genre,
SUM(CASE WHEN column_name = 'age' THEN value END) AS age
FROM "Cell" 
GROUP BY row_id;

SELECT COUNT(*) FROM 
( SELECT row_id, 
UPPER(CASE WHEN column_name = 'genre' THEN value END) AS GENDER,
SUM(CASE WHEN column_name = 'age' THEN value END) AS AGE
FROM "Cell" 
GROUP BY row_id) ;

SELECT AVG(AGE) FROM 
( SELECT row_id, 
UPPER(CASE WHEN column_name = 'genre' THEN value END) AS GENDER,
SUM(CASE WHEN column_name = 'age' THEN value END) AS AGE
FROM "Cell" 
GROUP BY row_id) ;
SELECT MAX(AGE) FROM 
( SELECT row_id, 
UPPER(CASE WHEN column_name = 'genre' THEN value END) AS GENDER,
SUM(CASE WHEN column_name = 'age' THEN value END) AS AGE
FROM "Cell" 
GROUP BY row_id) ;

SELECT * FROM "Cell" WHERE column_name = 'nom';

"""

class DynamiDBModelQuerySet(models.QuerySet):


    ####################################
    # METHODS THAT DO DATABASE QUERIES #
    ####################################


    def count(self):
        """
        Perform a SELECT COUNT() and return the number of records as an
        integer.

        If the QuerySet is already fully cached, return the length of the
        cached results set to avoid multiple SELECT COUNT(*) calls.
        """
        if self._result_cache is not None:
            return len(self._result_cache)

        return self.query.get_count(using=self.db)
    # NOK
    def count(self):
        # return self.rows.count()
        return Row.objects.filter(table__name=type(self).__name__).count()


    def get(self, *args, **kwargs):
        """
        Perform the query and return a single object matching the given
        keyword arguments.
        """
        clone = self.filter(*args, **kwargs)
        if self.query.can_filter() and not self.query.distinct_fields:
            clone = clone.order_by()
        num = len(clone)
        if num == 1:
            return clone._result_cache[0]
        if not num:
            raise self.model.DoesNotExist(
                "%s matching query does not exist." %
                self.model._meta.object_name
            )
        raise self.model.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!" %
            (self.model._meta.object_name, num)
        )
    # OK
    def get(self, **kwargs):
        ids = []
        table = Table.objects.get(name=type(self).__name__)
        cols_name = [col.name for col in Column.objects.filter(table=table)]
        if len(kwargs) > len(cols_name):
            raise ValueError("Params are match more than this table defined columns.")
        
        # Get cell primary_key__id from cells matching kwargs
        for key, val in kwargs.iteritems():
            if key in ['pk', 'id']:
                ids = ids + [cell.primary_key__id for cell in Cell.filter(primary_key__table=table, primary_key__id=val)]
            elif key in cols_name:
                ids = ids + [cell.primary_key__id for cell in Cell.filter(primary_key__table=table, value_type__name=key, value=val)]
            else:
                raise exceptions.FieldError(
                    "Invalid field name(s) for model %s: '%s'." %
                    (type(self).__name__, key)
                )
                
        # remove duplicate id
        ids = list(set(ids))
        # Generate desired result with pivot
        res = pivot(Cell.objects.filter(primary_key__id__in=ids), 'value_type__name', 'primary_key__id', 'value')
        num = len(res)
        if num == 1:
            return res[0]
        if not num:
            raise super(DynamiDBModelQuerySet,self).model.DoesNotExist(
                "%s matching query does not exist." %
                type(self).__name__
            )
        raise super(DynamiDBModelQuerySet,self).model.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!" %
            (type(self).__name__, num)
        )

    def create(self, **kwargs):
        """
        Create a new object with the given kwargs, saving it to the database
        and returning the created object.
        """
        obj = self.model(**kwargs)
        self._for_write = True
        obj.save(force_insert=True, using=self.db)
        return obj

    # OK
    def create(self, defaults=None, **kwargs):
        ids = []
        table = Table.objects.get(name=type(self).__name__)
        cols_name = [col.name for col in Column.filter(table=table)]

        lookup, params = self._extract_model_params(defaults, **kwargs)
        
        objs = []
        
        table_obj, table_created = Table.objects.get_or_create(name=type(self).__name__)
        
        if table_obj:
            row_obj, row_created = Row.objects.get_or_create(table=table_obj)
            if row_created:
                for attr, val in self.__dict__.iteritems():
                    col_obj, col_created = Column.objects.get_or_create(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=val))

        List_of_objects = Cell.objects.bulk_create(objs)
        
        return pivot(List_of_objects, 'value_type__name', 'primary_key__id', 'value')


    def get_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, creating one if necessary.
        Return a tuple of (object, created), where created is a boolean
        specifying whether an object was created.
        """
        lookup, params = self._extract_model_params(defaults, **kwargs)
        # The get() needs to be targeted at the write database in order
        # to avoid potential transaction consistency problems.
        self._for_write = True
        try:
            return self.get(**lookup), False
        except self.model.DoesNotExist:
            return self._create_object_from_params(lookup, params)

    # OK
    def get_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, creating one if necessary.
        Return a tuple of (object, created), where created is a boolean
        specifying whether an object was created.
        """
        lookup, params = self._extract_model_params(defaults, **kwargs)
        # The get() needs to be targeted at the write database in order
        # to avoid potential transaction consistency problems.
        try:
            return self.get(**lookup), False
        except self.model.DoesNotExist:
            return self._create_object_from_params(lookup, params)
    

    def update_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, updating one with defaults
        if it exists, otherwise create a new one.
        Return a tuple (object, created), where created is a boolean
        specifying whether an object was created.
        """
        defaults = defaults or {}
        lookup, params = self._extract_model_params(defaults, **kwargs)
        self._for_write = True
        with transaction.atomic(using=self.db):
            try:
                obj = self.select_for_update().get(**lookup)
            except self.model.DoesNotExist:
                obj, created = self._create_object_from_params(lookup, params)
                if created:
                    return obj, created
            for k, v in defaults.items():
                setattr(obj, k, v() if callable(v) else v)
            obj.save(using=self.db)
        return obj, False
    # OK
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
            #obj = self.select_for_update().get(**lookup)
            obj = self.get(**lookup)
        except super(DynamiDBModelQuerySet,self).model.DoesNotExist:
            obj, created = self._create_object_from_params(lookup, params)
            if created:
                return obj, created

        for attr, val in defaults.items():
            col_obj = Column.objects.get(table__name=type(self).__name__, name=attr)
            Cell.objects.filter(primary_key__table__name=type(self).__name__, primary_key__id=obj.primary_key__id, value_type=col_obj).update(value=val() if callable(val) else val)
        
        return pivot(Cell.objects.filter(primary_key__table__name=type(self).__name__, primary_key__id=obj.primary_key__id), 'value_type__name', 'primary_key__id', 'value'), False


    def _create_object_from_params(self, lookup, params):
        """
        Try to create an object using passed params. Used by get_or_create()
        and update_or_create().
        """
        try:
            with transaction.atomic(using=self.db):
                params = {k: v() if callable(v) else v for k, v in params.items()}
                obj = self.create(**params)
            return obj, True
        except IntegrityError as e:
            try:
                return self.get(**lookup), False
            except self.model.DoesNotExist:
                pass
            raise e
    # OK
    def _create_object_from_params(self, lookup, params):
        """
        Try to create an object using passed params. Used by get_or_create()
        and update_or_create().
        """
        try:
            params = {k: v() if callable(v) else v for k, v in params.items()}

            table_obj = Table.objects.get(name=type(self).__name__)
        
            row_obj = Row.objects.create(table=table_obj)
            if row_obj:
                for attr, val in params:
                    col_obj = Column.objects.get(table=table_obj, name=attr)
                    objs.append(Cell(primary_key=row_obj, value_type=col_obj, value=val))
                List_of_objects = Cell.objects.bulk_create(objs)
        
                return pivot(List_of_objects, 'value_type__name', 'primary_key__id', 'value')[0], True

        except IntegrityError as e:
            try:
                return self.get(**lookup), False
            except super(DynamiDBModelQuerySet,self).model.DoesNotExist:
                pass
            raise e


    def _extract_model_params(self, defaults, **kwargs):
        """
        Prepare `lookup` (kwargs that are valid model attributes), `params`
        (for creating a model instance) based on given kwargs; for use by
        get_or_create() and update_or_create().
        """
        defaults = defaults or {}
        lookup = kwargs.copy()
        for f in self.model._meta.fields:
            if f.attname in lookup:
                lookup[f.name] = lookup.pop(f.attname)
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        property_names = self.model._meta._property_names
        invalid_params = []
        for param in params:
            try:
                self.model._meta.get_field(param)
            except exceptions.FieldDoesNotExist:
                # It's okay to use a model's property if it has a setter.
                if not (param in property_names and getattr(self.model, param).fset):
                    invalid_params.append(param)
        if invalid_params:
            raise exceptions.FieldError(
                "Invalid field name(s) for model %s: '%s'." % (
                    self.model._meta.object_name,
                    "', '".join(sorted(invalid_params)),
                ))
        return lookup, params
    # OK
    def _extract_model_params(self, defaults, **kwargs):
        """
        Prepare `lookup` (kwargs that are valid model attributes), `params`
        (for creating a model instance) based on given kwargs; for use by
        get_or_create() and update_or_create().
        """
        cols_name = []
        defaults = defaults or {}
        lookup = kwargs.copy()

        table, created = Table.objects.get_or_create(name=type(self).__name__)

        if created:
            cols = []
            """
            cols = list(set(chain.from_iterable(
                (field.name, field.attname) if hasattr(field, 'attname') else (field.name,)
                for field in MyModel._meta.get_fields()
                # For complete backwards compatibility, you may want to exclude
                # GenericForeignKey from the results.
                if not (field.many_to_one and field.related_model is None)
            )))
            """
            for field in type(self)._meta.get_fields():
                cols.append(Column(table=table, name=field.name, type=type(field).__name__))
                cols_name.append(field.name)
            Column.objects.bulk_create(cols)

        else:
            cols_name = [col.name for col in Column.filter(table=table)]

        for field in cols_name:
            if field in lookup:
                lookup[field] = lookup.pop(field)
        params = {k: v for k, v in kwargs.items() if LOOKUP_SEP not in k}
        params.update(defaults)
        invalid_params = []
        for param in params:
            if param in cols_name:
                pass
            else:
                invalid_params.append(param)
        if invalid_params:
            raise exceptions.FieldError(
                "Invalid field name(s) for model %s: '%s'." % (
                    type(self).__name__,
                    "', '".join(sorted(invalid_params)),
                ))
        return lookup, params


    ##################################################################
    # PUBLIC METHODS THAT ALTER ATTRIBUTES AND RETURN A NEW QUERYSET #
    ##################################################################
    
    # Simple filter. Don't support complex query with Q and F
    def filter(self, **kwargs):
        ids = []
        cols_name = self.get_columns_name()
        if len(kwargs) > len(cols_name):
            raise ValueError("Params are match more than this table defined comlumns.")
        # Get cell primary_key__id from cells matching kwargs
        for key, val in kwargs.iteritems():
            if key in ['pk', 'id']:
                ids = ids + [cell.primary_key__id for cell in Cell.filter(primary_key__table=table, primary_key__id=val)]
            elif key in cols_name:
                ids = ids + [cell.primary_key__id for cell in Cell.filter(primary_key__table=table, value_type__name=key, value=val)]
            else:
                raise exceptions.FieldError(
                    "Invalid field name(s) for model %s: '%s'." %
                    (type(self).__name__, key)
                )
        # remove duplicate id
        ids = list(set(ids))
        # Generate desired result with pivot
        return pivot(Cell.objects.filter(primary_key__id__in=ids), 'value_type__name', 'primary_key__id', 'value')

    ###################
    # PRIVATE METHODS #
    ###################

    def _get_columns_name(self):
        table = Table.objects.get(name=type(self).__name__)
        return [col.name for col in Column.filter(table=table)]

    @staticmethod
    def _validate_values_are_expressions(values, method_name):
        invalid_args = sorted(str(arg) for arg in values if not hasattr(arg, 'resolve_expression'))
        if invalid_args:
            raise TypeError(
                'QuerySet.%s() received non-expression(s): %s.' % (
                    method_name,
                    ', '.join(invalid_args),
                )
            )

    def get_custom_annotation(self):
        # columns = Table.objects.get(name=type(self).__name__).columns.values('id','name')
        # OR
        columns = Table.objects.get(name=type(self).__name__).columns.all().values('id','name')

        return {
            column_name:Concat(Case(When(column_id=col_id, then=F('value')))
            for col_id, col_name in columns
        }


    #This API is called when there is a subquery. Injected tenant_ids for the subqueries.
    def _as_sql(self, connection):
        return super(DynamiDBModelQuerySet,self)._as_sql(connection)

class DynamiDBModelManager(models.Manager):
    """
    Generating custom initial queryset from pivoting datatable cell. 
    OK With this SQL syntaxe

    Trying to do this with Django ORM
    
    SELECT row_id AS id,
    (CASE WHEN column_name = 'xxx' THEN value END) AS column_name,
    ...
    AGGREGATION(CASE WHEN column_name = 'xxx' THEN value END) AS column_name,
    FROM "Cell"
    GROUP BY id;

    """
    def get_queryset(self):
    
        # Generate models columns annotation
        annotations = super(DynamiDBModelQuerySet,self)._get_custom_annotation()
        
        return DynamiDBModelQuerySet(self.model, using=self._db)
            .filter(entity=type(self).__name__)  # Important!
            .values('row_id')  # Important, # values + annotate => GROUP BY row_id
            .annotate(**annotations) # Annotate with columns_name
            .order_by() # Important

    # cs = Cell.objects.distinct().values('row_id').order_by().annotate(
    #     difficult=Case(When(column__name='difficult', then=F('value')),),
    #     hand=Case(When(column__name='hand', then=F('value')),),
    #     garden=Case(When(column__name='garden', then=F('value')),),
    #     parent=Case(When(column__name='parent', then=F('value')),),
    #     maintain=Case(When(column__name='maintain', then=F('value')),)
    # ).values_list('difficult', 'hand', 'garden', 'parent', 'maintain')

    # def get_queryset(self):
    #     return super().get_queryset()
    #         .filter(entity=type(self).__name__) # Important
    #         .annotate()
    #         .order_by() # Important

    # def pdfs(self):
    #     return self.get_queryset().pdfs()

    # def smaller_than(self, size):
    #     return self.get_queryset().smaller_than(size)

    # def update(self, **kwargs):
    #     self.add_tenant_filters_without_joins()
    #     #print(self.query.alias_refcount)
    #     return super(TenantQuerySet,self).update(**kwargs)
    
class Document(models.Model):
    name = models.CharField(max_length=30)
    size = models.PositiveIntegerField(default=0)
    file_type = models.CharField(max_length=10, blank=True)

    objects = DocumentManager()

class Table(models.Model):

    name = models.CharField(max_length=30)
    description = models.TextField(null=True)
    
    # table_parent = models.ForeignKey('Table', on_delete=models.CASCADE, related_name='table_parent')
    
    """ cell_collection = models.ManyToManyField(Cell, through='Column', related_name='+') """

    def __str__(self):
        return self.name
    
    """
    @property
    def column_collection(self):
        # Return list of columns
        for table in self:
            columns = [{"name":col.name, "label":col.label, "options": col.options, "nullable":col.nullable} for col in Column.filter(table=table)]
            return columns

    @property
    def row_collection(self):
        # Return list of columns
        for table in self:
            rows = Row.filter(table=table).cell_collection
            return rows
    """

class Column(models.Model):

    TEXT = 'text'
    CHECKBOX = 'checkbox'
    TEXTAREA = 'textarea'
    RADIO = 'radio'
    SELECT = 'select'
    DATE = 'date'
    DATETIME = 'datetime'
    NUMBER = 'number'
    FIELD_TYPE_CHOICES = (
        (TEXT, 'Text'),
        (TEXTAREA, 'Text zone'),
        (NUMBER, 'Number'),
        (CHECKBOX, 'Checkboxe (for many options with multi choices)'),
        (RADIO, 'Radio button (for many options with single choice. Up to 3 options)'),
        (SELECT, 'Radio button (for many options with single choice. More than 3 options)'),
        (DATE, 'Date (Calendar)'),
        (DATETIME, 'Date and time (Calendar)'),
    )
    
    """
    type = models.CharField(
        choices=FIELD_TYPE_CHOICES,
        default=TEXT,
        max_length=100
    )
    """
    name = models.CharField(max_length=100)
    type = models.CharField(max_length=100)
    # label = models.CharField(max_length=30, blank=True, null=True)
    # options = models.TextField(blank=True, null=True)
    # nullable = models.BooleanField(default=True)
    
    table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='columns')
    
    # cell = models.ForeignKey('Cell', on_delete=models.CASCADE)
    
    def __str__(self):
        return self.name


class Row(models.Model):

    table = models.ForeignKey(Table, on_delete=models.CASCADE, related_name='rows')
    
    cells = models.ManyToManyField(Column, through='Cell', related_name='+')

    def __str__(self):
        return self.name
    """
    @property
    def cell_collection(self):
        # Return list of columns
        for row in self:
            cells = [{"type":cell.column.type, "value":cell.value} for cell in Cell.filter(row=row)]
            return cells
    """

class Cell(models.Model):

    primary_key = models.ForeignKey('Row', on_delete=models.CASCADE)
    value_type = models.ForeignKey('Column', on_delete=models.CASCADE)
    value = models.CharField(max_length=500)

    c = Cell.objects.annotate(id=Case(When(column__name='', then=Value('5%')), ), )

cs = Cell.objects.distinct().values('row_id').order_by().annotate(
    difficult=Case(When(column__name='difficult', then=F('value')),),
    hand=Case(When(column__name='hand', then=F('value')),),
    garden=Case(When(column__name='garden', then=F('value')),),
    parent=Case(When(column__name='parent', then=F('value')),),
    maintain=Case(When(column__name='maintain', then=F('value')),)
).values_list('difficult', 'hand', 'garden', 'parent', 'maintain')

#Modified QuerySet to add suitable filters on joins.
class TenantQuerySet(models.QuerySet):
    
    #This is adding tenant filters for all the models in the join.
    def add_tenant_filters_with_joins(self):
        current_tenant=get_current_tenant()
        if current_tenant:
            extra_sql=[]
            extra_params=[]
            current_table_name=self.model._meta.db_table
            alias_refcount = self.query.alias_refcount
            alias_map = self.query.alias_map
            for k,v in alias_refcount.items():
                if(v>0 and k!=current_table_name):
                    current_model=get_model_by_db_table(alias_map[k].table_name)
                    extra_sql.append('"'+k+'"."'+current_model.tenant_id+'" = %s')
                    extra_params.append(current_tenant.id)
            self.query.add_extra([],[],extra_sql,extra_params,[],[])
    
    def add_tenant_filters_without_joins(self):
        current_tenant=get_current_tenant()
        if current_tenant:
            l=[]
            table_name=self.model._meta.db_table
            l.append(table_name+'.'+self.model.tenant_id+'='+str(current_tenant.id))
            self.query.add_extra([],[],l,[],[],[])

    #Below are APIs which generate SQL, this is where the tenant_id filters are injected for joins.
    
    def __iter__(self):
        self.add_tenant_filters_with_joins()
        return super(TenantQuerySet,self).__iter__()
    
    def aggregate(self, *args, **kwargs):
        self.add_tenant_filters_with_joins()
        return super(TenantQuerySet,self).aggregate(*args, **kwargs)

    def count(self):
        self.add_tenant_filters_with_joins()
        return super(TenantQuerySet,self).count()

    def get(self, *args, **kwargs):
        #self.add_tenant_filters()
        self.add_tenant_filters_with_joins()
        return super(TenantQuerySet,self).get(*args,**kwargs)

    # def get_or_create(self, defaults=None, **kwargs):
    #     self.add_tenant_filters()
    #     return super(TenantQuerySet,self).get_or_create(defaults,**kwargs)

    # def update(self, **kwargs):
    #     self.add_tenant_filters_without_joins()
    #     #print(self.query.alias_refcount)
    #     return super(TenantQuerySet,self).update(**kwargs)
    
    # def _update(self, values):
    #     self.add_tenant_filters_without_joins()
    #     #print(self.query.alias_refcount)
    #     return super(TenantQuerySet,self)._update(values)
    
    #This API is called when there is a subquery. Injected tenant_ids for the subqueries.
    def _as_sql(self, connection):
        self.add_tenant_filters_with_joins()
        return super(TenantQuerySet,self)._as_sql(connection)

#Below is the manager related to the above class. 
class TenantManager(TenantQuerySet.as_manager().__class__):
    #Injecting tenant_id filters in the get_queryset.
    #Injects tenant_id filter on the current model for all the non-join/join queries. 
    def get_queryset(self):
        current_tenant=get_current_tenant()
        if current_tenant:
            kwargs = { self.model.tenant_id: current_tenant.id}
            return super(TenantManager, self).get_queryset().filter(**kwargs)
        return super(TenantManager, self).get_queryset()

#Abstract model which all the models related to tenant inherit.
class TenantModel(models.Model):

    #New manager from middleware
    objects = TenantManager()
    tenant_id=''

    #adding tenant filters for save
    #Citus requires tenant_id filters for update, hence doing this below change.
    def _do_update(self, base_qs, using, pk_val, values, update_fields, forced_update):
        current_tenant=get_current_tenant()
        if current_tenant:
            kwargs = { self.__class__.tenant_id: current_tenant.id}
            base_qs = base_qs.filter(**kwargs)
        return super(TenantModel,self)._do_update(base_qs, using, pk_val, values, update_fields, forced_update)

    class Meta:
        abstract = True

def get_current_user():
    """
    Despite arguments to the contrary, it is sometimes necessary to find out who is the current
    logged in user, even if the request object is not in scope.  The best way to do this is 
    by storing the user object in middleware while processing the request.
    """
    return getattr(_thread_locals, 'user', None)

def get_model_by_db_table(db_table):
    for model in apps.get_models():
        if model._meta.db_table == db_table:
            return model
    else:
        # here you can do fallback logic if no model with db_table found
        raise ValueError('No model found with db_table {}!'.format(db_table))
        # or return None

def get_current_tenant():
    """
    To get the Tenant instance for the currently logged in tenant.
    example:
        tenant = get_current_tenant()
    """
    tenant = getattr(_thread_locals, 'tenant', None)

    # tenant may not be set yet, if request user is anonymous, or has no profile,
    # if not tenant:
    #     set_tenant_to_default()
    
    return getattr(_thread_locals, 'tenant', None)


# def set_tenant_to_default():
#     """
#     Sets the current tenant as per BASE_TENANT_ID.
#     """
#     # import is done from within the function, to avoid trouble 
#     from models import Tenant, BASE_TENANT_ID
#     set_current_tenant( Tenant.objects.get(id=BASE_TENANT_ID) )
    

def set_current_tenant(tenant):
    setattr(_thread_locals, 'tenant', tenant)


class ThreadLocals(object):
    """Middleware that gets various objects from the
    request object and saves them in thread local storage."""
    def process_request(self, request):
        _thread_locals.user = getattr(request, 'user', None)

        # Attempt to set tenant
        if _thread_locals.user and not _thread_locals.user.is_anonymous():
            try:
                profile = _thread_locals.user.get_profile()
                if profile:
                    _thread_locals.tenant = getattr(profile, 'tenant', None)
            except:
                raise ValueError(
                    """A User was created with no profile.  For security reasons, 
                    we cannot allow the request to be processed any further.
                    Try deleting this User and creating it again to ensure a 
                    UserProfile gets attached, or link a UserProfile 
                    to this User.""")
