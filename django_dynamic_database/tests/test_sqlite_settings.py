BACKEND = 'sqlite'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': 'dynamic_db_sqlite',
    }
}

INSTALLED_APPS = (
    'django_dynamic_database',
    'django_dynamic_database.tests.dynamic_database',
)

SITE_ID = 1,

SECRET_KEY = 'secret'

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
)