from __future__ import unicode_literals

# VERSION = (0, 9, 0)
# __version__ = '.'.join(str(v) for v in VERSION)

default_app_config = "django_dynamic_database.apps.DynamicDBConfig"


def register(*args, **kwargs):
    """
    Registers a model class as an MPTTModel, adding MPTT fields and adding MPTTModel to __bases__.
    This is equivalent to just subclassing MPTTModel, but works for an already-created model.
    """
    from django_dynamic_database.django_dynamic_database import DynamicDBModel
    return DynamicDBModel.register(*args, **kwargs)


class AlreadyRegistered(Exception):
    "Deprecated - don't use this anymore. It's never thrown, you don't need to catch it"
