from django.core import exceptions
from django.apps import apps
from django.db import models
from django.db.models import Avg, Q, F
from django_pivot.pivot import pivot
try:
    from threading import local
except ImportError:
    from django.utils._threading_local import local

from collections import OrderedDict
import pdb

_thread_locals = local()


class DynamiDBModelQuerySet(models.QuerySet):
    def count(self):
        return self.rows.count()

    def all(self, size):
        # Get instance class name => primary_key__table__name = type(self).__name__
        return pivot(Cell.objects.filter(primary_key__table__name=type(self).__name__), 'value_type__name', 'primary_key__id', 'value')
    
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

    def get(self, **kwargs):
        ids = []
        table = Table.objects.get(name=type(self).__name__)
        cols_name = [col.name for col in Column.filter(table=table)]
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
        res = pivot(Cell.objects.filter(primary_key__id__in=ids), 'value_type__name', 'primary_key__id', 'value')
        num = len(res)
        if num == 1:
            return res[0]
        if not num:
            raise self.model.DoesNotExist(
                "%s matching query does not exist." %
                type(self).__name__
            )
        raise self.model.MultipleObjectsReturned(
            "get() returned more than one %s -- it returned %s!" %
            (type(self).__name__, num)
        )


    # Simple filter. Don't support complex query with Q and F
    def filter(self, **kwargs):
        ids = []
        # Get cell primary_key__id from cells matching kwargs
        for key, val in kwargs.iteritems():
            if key in ['pk', 'id']:
                ids = ids + [cell.primary_key__id for cell in Cell.filter(primary_key__table__name=type(self).__name__, primary_key__id=val)]
            else:
                ids = ids + [cell.primary_key__id for cell in Cell.filter(primary_key__table__name=type(self).__name__, value_type__name=key, value=val)]
        # remove duplicate id
        ids = list(set(ids))
        # Generate desired result with pivot
        return pivot(Cell.objects.filter(primary_key__id__in=ids), 'value_type__name', 'primary_key__id', 'value')

    # def __iter__(self):
    #     return super(PivotModelQuerySet,self).__iter__()

    # var is a Function => callable(var) , ou if isinstance(filter_obj, Q):
    # Get function arg name => func.__code__.co_varnames

    # Get function arg value
    # import inspect
    # def func(a, b, c):
    #    frame = inspect.currentframe()
    #    args, _, _, values = inspect.getargvalues(frame)
    #    print 'function name "%s"' % inspect.getframeinfo(frame)[2]
    #    for i in args:
    #        print "    %s = %s" % (i, values[i])
    #   return [(i, values[i]) for i in args]

    def aggregate(self, *args, **kwargs):
        return pivot(Cell.objects.filter(primary_key__table__name=type(self).__name__), 'value_type__name', 'primary_key__id', 'value', Avg)

    # Simple values_list. Don't support bultin function eg. Entry.objects.values_list('id', Lower('headline'))
    def values_list(self, *fields, flat=False, named=False):
        return pivot(Cell.filter(primary_key__table__name=type(self).__name__, value_type__name__in=fields), 'value_type__name', 'primary_key__id', 'value')

    def create(self, defaults=None, **kwargs):

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

    def bulk_create(self, objs, batch_size=None):
        return objs

    def get_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, creating one if necessary.
        Return a tuple of (object, created), where created is a boolean
        specifying whether an object was created.
        ""
        lookup, params = self._extract_model_params(defaults, **kwargs)
        # The get() needs to be targeted at the write database in order
        # to avoid potential transaction consistency problems.
        self._for_write = True
        try:
            return self.get(**lookup), False
        except self.model.DoesNotExist:
            return self._create_object_from_params(lookup, params)
        """
        pass
    
    def update_or_create(self, defaults=None, **kwargs):
        """
        Look up an object with the given kwargs, updating one with defaults
        if it exists, otherwise create a new one.
        Return a tuple (object, created), where created is a boolean
        specifying whether an object was created.
        ""
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
        """
        pass

    def earliest(self, *fields, field_name=None):
        # return self._earliest_or_latest(*fields, field_name=field_name)
        pass
    
    def latest(self, *fields, field_name=None):
        # return self.reverse()._earliest_or_latest(*fields, field_name=field_name)
        pass
    
    def first(self):
        """Return the first object of a query or None if no match is found.""
        for obj in (self if self.ordered else self.order_by('pk'))[:1]:
            return obj
        """
        pass
    
    def last(self):
        """Return the last object of a query or None if no match is found.""
        for obj in (self.reverse() if self.ordered else self.order_by('-pk'))[:1]:
            return obj
        """
        pass
    
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
        return super(DynamiDBModelQuerySet,self)._as_sql(connection)

# pivot_table = pivot(ShirtSales.objects.filter(style='Golf'), 'region', 'shipped', 'units')
class DynamiDBModelManager(models.Manager):
    def get_queryset(self):
        return DynamiDBModelQuerySet(self.model, using=self._db)  # Important!

    def pdfs(self):
        return self.get_queryset().pdfs()

    def smaller_than(self, size):
        return self.get_queryset().smaller_than(size)

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
    NUMBER = 'number'
    FIELD_TYPE_CHOICES = (
        (TEXT, 'Text'),
        (TEXTAREA, 'Text zone'),
        (NUMBER, 'Number'),
        (CHECKBOX, 'Checkboxe (for many options with multi choices)'),
        (RADIO, 'Radio button (for many options with single choice. Up to 3 options)'),
        (SELECT, 'Radio button (for many options with single choice. More than 3 options)'),
        (DATE, 'Date (Calendar)'),
    )
    
    type = models.CharField(
        choices=FIELD_TYPE_CHOICES,
        default=TEXT,
        max_length=100
    )
    name = models.CharField(max_length=30)
    label = models.CharField(max_length=30)
    options = models.TextField(blank=True, null=True)
    nullable = models.BooleanField(default=True)
    
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
