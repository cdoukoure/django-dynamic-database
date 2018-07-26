from __future__ import absolute_import
from django.db.models import CharField, Func, F, Avg, DecimalField

from django.conf import settings
from django.db.models import ExpressionWrapper
from django.test import TestCase
from django.utils.encoding import force_text

from .models import Student
from django_dynamic_database.django_dynamic_database import Table, Column, Row, Cell, DynamicDBModel


class Tests(TestCase):

    @classmethod
    def setUpClass(cls):
        super(Tests, cls).setUpClass()
        # Generate a bunch of data to pivot
        Student(first_name='Neo', last_name='Dicker').save()
        Student(first_name='John', last_name='Witch').save()
        Student(first_name='Jean', last_name='Reno').save()
        Student(first_name='Jena-Claude', last_name='Van Dam').save()
        Student(first_name='Arnaud', last_name='Migan').save()
        Student(first_name='Micheal', last_name='Jackson').save()
        Student(first_name='Kelly', last_name='Robert').save()
        Student(first_name='Jet', last_name='Lee').save()
        Student(first_name='Tonny', last_name='Stark').save()

    def test_table_creation(self):
        table = Table.objects.get(name='student')

        self.assertEqual(table.name, 'student')
    


