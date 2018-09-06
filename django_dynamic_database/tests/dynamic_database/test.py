from __future__ import absolute_import
import datetime

from django.test import TestCase, Client, RequestFactory, override_settings
from django.urls import reverse
from django.utils import timezone
from django.db import models
from django_dynamic_database.models import Table, Row, Column, Cell
from django_dynamic_database.django_dynamic_database import DynamicDBModel, Sum

from django.contrib.auth.models import User
# from django.contrib.auth import get_user_model

# User = get_user_model()

class KingBook(DynamicDBModel):
    name = models.CharField(max_length=40)
    rate = models.FloatField(max_length=20, default=1.0)
    weight = models.FloatField(max_length=20, null=True, blank=True)


@override_settings(ROOT_URLCONF='django_dynamic_database.urls')
class DynamicDBModelModelTests(TestCase):

    def setUp(self):
        # Every test needs access to the request factory.
        self.factory = RequestFactory()
        
        # Every test needs a client.
        self.client = Client()
        
        # Create two users
        self.test_user1 = User.objects.create_user(username='testuser1', email='testuser1@dynamicdb.xyz')
        self.test_user2 = User.objects.create_user(username='testuser2', email='testuser2@dynamicdb.xyz')
        
        self.test_user1.set_password('12345')
        self.test_user2.set_password('12345')
        
        self.test_user1.is_active = True
        self.test_user2.is_active = True
        
        self.test_user1.save()
        self.test_user2.save()
        
        # users = User.objects.all()
        
        # for us in users:
        #     print(us)
        
        cells = []
        
        # Create tables
        tables = []
        tables.append(Table.objects.create(name="testTable_1"))
        tables.append(Table.objects.create(name="testTable_2"))

        for t in tables:
            # Create tables columns
            cols = []
            for i in range(1,5):
                cols.append(Column(table=t, name="col_" + str(i)))
            Column.objects.bulk_create(cols)

            # Create 30 BookInstance objects
            number_of_rows = 30
            for row in range(number_of_rows):
                r = Row.objects.create(table=t)
                for col in t.columns.all():
                    cells.append(Cell(primary_key=r, value_type=col, value="value " + str(col.id)))
        Cell.objects.bulk_create(cells)

    """
    # def test_create_model_instance(self):
    #     t = Table.objects.create(name="testTable")
    #     self.assertEqual(t.name, "testTable")
    
    
    def test_create_table_columns_rows_cells_instance(self):
    
        t = Table.objects.create(name="testTable_2")
        
        cols = []
        for i in range(1,5):
            cols.append(Column(table=t, name="col_" + str(i)))
        Column.objects.bulk_create(cols)

        t = Table.objects.get(name="testTable_2")
        
        r = Row.objects.create(table=t)
        
        cells = []
        for col in t.columns.all():
            cells.append(Cell(primary_key=r, value_type=col, value="value " + str(col.id)))
        Cell.objects.bulk_create(cells)

        self.assertEqual(len(cols), len(cells))
        
        # cols = Column.objects.all()
        
        # print(str(cols.__dict__))
        
        # queryset_methods = [method_name for method_name in dir(cols) if callable(getattr(cols, method_name))]
        
        # print(str(queryset_methods))
    """
    
    def test_create_dynamic_db_model_instance(self):
        
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
        self.assertEqual(bk7.id, 63)
        
        # Support MyModel.objects.count()
        all = KingBook.objects.all()
        cnt = KingBook.objects.count()
        self.assertEqual(len(all), cnt)

        # Support Save method
        bk8 = KingBook(name="Brad Pete")
        bk8.save()
        
        
        # Support default value
        bk9 = KingBook.objects.get(id=64)
        self.assertEqual(bk9.name, "Brad Pete")
        self.assertEqual(bk9.rate, '1.0')
        self.assertEqual(bk9.weight, 'None')

        # all1 = KingBook.objects.all()
        # print(all1)

        # Support instance update
        bk9.rate = 1.33
        bk9.save()
        bk10 = KingBook.objects.get(name="Brad Pete")
        self.assertEqual(bk10.rate, '1.33')

        # all2 = KingBook.objects.all()
        # print(all2)

        # Support complex filter
        bk11 = KingBook.objects.filter(id__gt=62)
        self.assertEqual(len(bk11), 2)
        
        
        # Support Aggregation
        higher_rate = KingBook.objects.aggregate(models.Max('rate'))
        self.assertEqual(higher_rate, {'rate__max': '5'})
        lower_rate = KingBook.objects.aggregate(models.Min('rate'))
        self.assertEqual(lower_rate, {'rate__min': '1.33'})
        
        # SUM bugs with Postgres db
        # sum_rate = KingBook.objects.aggregate(models.Sum('rate'))
        # self.assertEqual(sum_rate, {'rate__sum': 13.33})
        
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
        bk20 = KingBook.objects.filter(id__lt=66).update(rate=1.5)
        bk12 = KingBook.objects.get(name="Tony Stark2")
        self.assertEqual(bk12.rate, '1.5')


    def test_login(self):
        login = self.client.login(username='testuser1', password='12345')
        self.assertTrue(login)
    
    """
    def test_details(self):
        # Create an instance of a GET request.
        request = self.factory.get('/customer/details')

        # Recall that middleware are not supported. You can simulate a
        # logged-in user by setting request.user manually.
        request.user = self.testuser1

        # Test my_view() as if it were deployed at /customer/details
        response = tables_view(request)
        self.assertEqual(response.status_code, 200)

    """
    
    def test_views(self):
        """
        The detail view of a question with a pub_date in the future
        returns a 404 not found.
        """
        login = self.client.login(username='testuser1', password='12345')
        
        print(login)

        t = Table.objects.get(name="testTable_1")
        
        url_table_list = reverse('tables')
        response = self.client.get(url_table_list)
        # print(response.content)
        self.assertEqual(response.status_code, 200)

        url_table_details = reverse('table-details', args=(t.id,))
        response = self.client.get(url_table_details)
        # print(response.content)
        self.assertEqual(response.status_code, 200)
        

        url_table_table_rows = reverse('table-rows', args=(t.id,))
        response = self.client.get(url_table_table_rows)
        # print(response.content)
        self.assertEqual(response.status_code, 200)
        
        # self.client.login(username='c.doukoure@outlook.fr', password='pianniste')
        
        response = self.client.post(url_table_table_rows, {table_id:t.id, col_1:'test col_1', col_2:'test col_2', col_3:'test col_3', col_4:'test col_4', col_5:'test col_5'}) # blank data dictionary
        print(response.content)
        self.assertEqual(response.status_code, 200)
        """
        url_table_row_details = reverse('django_dynamic_database:table-row-details', args=(t3.id, r.id,))
        response = self.client.get(url_table_row_details)
        print(response.content)
        self.assertEqual(response.status_code, 200)
        """


    
