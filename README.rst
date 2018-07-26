.. image:: https://img.shields.io/travis/django-polymorphic/django-polymorphic-tree/master.svg?branch=master
    :target: http://travis-ci.org/django-polymorphic/django-polymorphic-tree
.. image:: https://img.shields.io/pypi/v/django-polymorphic-tree.svg
    :target: https://pypi.python.org/pypi/django-polymorphic-tree/
.. image:: https://img.shields.io/badge/wheel-yes-green.svg
    :target: https://pypi.python.org/pypi/django-polymorphic-tree/
.. image:: https://img.shields.io/pypi/l/django-polymorphic-tree.svg
    :target: https://pypi.python.org/pypi/django-polymorphic-tree/
.. image:: https://img.shields.io/codecov/c/github/django-polymorphic/django-polymorphic-tree/master.svg
    :target: https://codecov.io/github/django-polymorphic/django-polymorphic-tree?branch=master

django-dynamic-database
=======================

This package use django-pivot.
You can write several Django models handling on 4 tables in your database.

Installation
============

First install the module, preferably in a virtual environment::

    pip install django-dynamic-database

Or install the current repository::

    pip install -e git+https://github.com/django-polymorphic/django-polymorphic-tree.git#egg=django-dynamic-database


Configuration
-------------

Next, create a project which uses the application::

    cd ..
    django-admin.py startproject demo

Add the following to ``settings.py``:

.. code:: python

    INSTALLED_APPS += (
        'django_dynamic_database',
    )

And then

.. code:: python

    from django_dynamic_database.models import DynamicDBModel

Usage
-----

The main feature of this module is creating a tree of custom node types.
It boils down to creating a application with 2 files:

The ``models.py`` file should define the custom model type, and any fields it has:

.. code:: python

    from django_dynamic_database.models import DynamicDBModel, DynamicDBCharField, DynamicDBForeignKey


    class Course(DynamicDBModel):
        title = DynamicDBCharField(_("Title"), max_length=200)


    class Session(DynamicDBModel):
        course = DynamicDBForeignKey(_("Opening title"), max_length=200)
        title = DynamicDBCharField(_("Title"), max_length=200)


The ``admin.py`` file should define the admin, both for the child nodes and parent:

.. code:: python

    from django.contrib import admin
    from django.utils.translation import ugettext_lazy as _
    from django_dynamic_database.admin import DynamicDBModelAdmin, DynamicDBModelAdmin


Tests
-----


Todo
----

* Relational models links (objects prefetch_related, select_related)
* Delete data in database when models is deleted (synchronize with makemigration and migrate)


Contributing
------------

This module is designed to be generic. In case there is anything you didn't like about it,
or think it's not flexible enough, please let us know. We'd love to improve it!

If you have any other valuable contribution, suggestion or idea,
please let us know as well because we will look into it.
Pull requests are welcome too. :-)


