from __future__ import absolute_import
import datetime

from django.test import TestCase
from django.utils import timezone
from django.db import models
from django_dynamic_database.models import Table, Row, Column, Cell
from django_dynamic_database.django_dynamic_database import DynamicDBModel, Sum



class DynamicDBModelModelTests(TestCase):

    def test_create_model_instance(self):
        t = Table.objects.create(name="testTable")
        self.assertEqual(t.name, "testTable")
    
    
    def test_create_table_columns_rows_cells_instance(self):
    
        t = Table.objects.create(name="test_table_2")
        
        cols = []
        for i in range(1,5):
            cols.append(Column(table=t, name="col_" + str(i)))
        Column.objects.bulk_create(cols)

        t = Table.objects.get(name="test_table_2")
        
        r = Row.objects.create(table=t)
        
        cells = []
        for col in t.columns.all():
            cells.append(Cell(primary_key=r, value_type=col, value="value " + str(col.id)))
        Cell.objects.bulk_create(cells)

        self.assertEqual(len(cols), len(cells))
        
        cols = Column.objects.all()
        
        # print(str(cols.__dict__))
        
        # queryset_methods = [method_name for method_name in dir(cols) if callable(getattr(cols, method_name))]
        
        # print(str(queryset_methods))
    
    
    def test_create_dynamic_db_model_instance(self):
        
        class KingBook(DynamicDBModel):
            name = models.CharField(max_length=40)
            rate = models.FloatField(max_length=20, default=1.0)
            weight = models.FloatField(max_length=20, null=True, blank=True)
        
        
        # Support MyModel.objects.none()
        empt = KingBook.objects.all()
        self.assertEqual(str(empt), '<QuerySet []>')

        # Support MyModel.objects.create()
        bk1 = KingBook.objects.create(name="Tony Stark", rate=3.5)
        
        bk2 = KingBook.objects.create(name="John Wick", rate=5)
        
        # Not yet support default value
        # bk3 = KingBook.objects.create(name="Jet Lee")
        
        self.assertEqual(bk1.__class__.__name__, "KingBook")
        self.assertEqual(bk2.name, "John Wick")
        # self.assertEqual(bk3.rate, 1.0)

        # Support MyModel.objects.get()
        bk5 = KingBook.objects.get(name="Tony Stark")
        self.assertEqual(bk5.name, bk1.name)
        
        # Support MyModel.objects.get_or_create()
        bk6, bk6_created = KingBook.objects.get_or_create(name="John Wick") # Get
        self.assertEqual(bk6.name, "John Wick")
        bk7, bk7_created = KingBook.objects.get_or_create(name="Will Smith", rate=3.5)  # Create
        self.assertEqual(bk7.id, 3)
        
        # Support MyModel.objects.count()
        all = KingBook.objects.all()
        cnt = KingBook.objects.count()
        self.assertEqual(len(all), cnt)

        # Support Save method
        bk8 = KingBook(name="Brad Pete")
        bk8.save()
        
        
        # Support default value
        bk9 = KingBook.objects.get(id=4)
        self.assertEqual(bk9.name, "Brad Pete")
        self.assertEqual(bk9.rate, '1.0')
        self.assertEqual(bk9.weight, 'None')

        all1 = KingBook.objects.all()
        print(all1)

        # Support instance update
        bk9.rate = 1.33
        bk9.save()
        bk10 = KingBook.objects.get(name="Brad Pete")
        self.assertEqual(bk10.rate, '1.33')

        all2 = KingBook.objects.all()
        print(all2)

        # Support filter
        bk11 = KingBook.objects.filter(id__gt=2)
        self.assertEqual(len(bk11), 2)
        
        # Support Aggregation
        higher_rate = KingBook.objects.aggregate(models.Max('rate'))
        self.assertEqual(higher_rate, {'rate__max': '5'})
        lower_rate = KingBook.objects.aggregate(models.Min('rate'))
        self.assertEqual(lower_rate, {'rate__min': '1.33'})
        
        # SUM bugs with Postgres db
        sum_rate = KingBook.objects.aggregate(models.Sum('rate'))
        self.assertEqual(sum_rate, {'rate__sum': 9.83})
        
        # Support delete()
        bk12 = KingBook.objects.create(name="Tony Stark2", rate=3.5)
        bk13 = KingBook.objects.create(name="John Wick1", rate=5)
        bk14 = KingBook.objects.create(name="John Wick2", rate=5)
        bk15 = KingBook.objects.create(name="John Wick3", rate=5)
        bk16 = KingBook.objects.create(name="John Wick4", rate=5)
        bk17 = KingBook.objects.create(name="John Wick5", rate=5)
        bk18 = KingBook.objects.create(name="John Wick6", rate=5)
        bk19 = KingBook.objects.create(name="John Wick7", rate=5)

        deleted = KingBook.objects.get(name="John Wick6").delete()
        
        self.assertEqual(deleted, (8, {'django_dynamic_database.KingBook': 8}))
        
        # Support update()
        bk20 = KingBook.objects.filter(id__lt=7).update(rate=1.5)
        bk12 = KingBook.objects.get(name="Tony Stark2")
        self.assertEqual(bk12.rate, '1.5')


    
