from setuptools import setup, find_packages

setup(
    name='django-dynamic-database',
    version='0.1.0',
    description='Create dynamic physical database with pivot table concept',
    long_description=read('README.rst'),
    url='https://github.com/cdoukoure/django-pivot-models',
    download_url='https://github.com/cdoukoure/django-pivot-models/archive/master.zip',
    author='Jean-Charles DOUKOURE',
    author_email='c.doukoure@outlook.fr',
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    packages=find_packages(),
    install_requires=['django>=1.11']
)
