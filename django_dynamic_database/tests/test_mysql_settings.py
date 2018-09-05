BACKEND = 'mysql'

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'dynamic_db',
    }
}

INSTALLED_APPS = (
    'django.contrib.auth',
    'django_dynamic_database',
    'django_dynamic_database.tests.dynamic_database',
)

SITE_ID = 1

SECRET_KEY = 'secret'

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
)