from __future__ import unicode_literals

from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class DynamicDBConfig(AppConfig):
    name = "django_dynamic_database"
    verbose_name = _("django_dynamic_database")

    """
    def ready(self):
        from django.core.management import call_command
        from .models import InstanceIDModel
        from morango.certificates import ScopeDefinition
        from .signals import add_to_deleted_models  # noqa: F401

        # NOTE: Warning: https://docs.djangoproject.com/en/1.10/ref/applications/#django.apps.AppConfig.ready
        # its recommended not to execute queries in this method, but we are producing the same result after the first call, so its OK

        # call this on app load up to get most recent system config settings
        try:
            InstanceIDModel.get_or_create_current_instance()
            if not ScopeDefinition.objects.filter():
                call_command("loaddata", "scopedefinitions")
        # we catch this error in case the database has not been migrated, b/c we can't query it until its been created
        except (OperationalError, ProgrammingError):
            pass
    """

