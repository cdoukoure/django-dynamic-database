BACKEND = 'postgres'

DATABASES = {
    'default': {
        # 'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'dynamic_db',
    }
}

INSTALLED_APPS = (
    'django_dynamic_database',
    'django_dynamic_database.tests.dynamic_database',
)


SECRET_KEY = 'secret'

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
)