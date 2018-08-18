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
    
    
    def test_create_dynamic_db_model_instance(self):
        
        class KingBook(DynamicDBModel):
            name = models.CharField(max_length=40)
            rate = models.FloatField(max_length=20,  default=1.0)
        
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
        bk7, bk7_created = KingBook.objects.get_or_create(name="Will Smith")  # Create
        self.assertEqual(bk7.id, 3)
        
        # Support MyModel.objects.count()
        all = KingBook.objects.all()
        cnt = KingBook.objects.count()
        self.assertEqual(len(all), cnt)
    

        # Support Save method
        bk8 = KingBook(name="Brad Pete")
        bk8.save()
        
        # Support default value
        bk8 = KingBook.objects.get(name="Brad Pete")
        self.assertEqual(bk8.rate, 1.0)

        bk8.rate = 2.33
        bk8.save()
        bk8 = KingBook.objects.get(name="Brad Pete")
        self.assertEqual(bk8.rate, 2.33)

        # Support Aggregation
        higher_rate = KingBook.objects.aggregate(models.Max('rate'))
        self.assertEqual(higher_rate, {'rate__max': '5'})
        higher_rate = KingBook.objects.aggregate(models.Min('rate'))
        self.assertEqual(higher_rate, {'rate__min': '1.0'})
        # SUM bugs with Postgres db
        sum_rate = KingBook.objects.aggregate(models.Sum('rate'))
        self.assertEqual(sum_rate, {'rate__sum': 8.5})
    
    

