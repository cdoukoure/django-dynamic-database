import re
from django.db import models

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


