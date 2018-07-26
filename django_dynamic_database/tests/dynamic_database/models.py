from django.db import models
from django_dynamic_database import DynamicDBModel

class Student(DynamicDBModel):
    first_name = models.CharField(max_length=20)
    last_name = models.CharField(max_length=50)
