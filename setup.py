from setuptools import setup, find_packages


with open("README.rst", "r") as fh:
    long_description = fh.read()


setup_params = dict(
    name =          'pysqream',
    version =       '3.0.0',
    description =   'DB-API connector for SQreamDB', 
    long_description = long_description,
    url = "https://github.com/SQream/pysqream",
    author = "SQream Technologies",
    author_email = "info@sqream.com",
    packages = ['pysqream'], 
    classifiers = [
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    keywords = 'database db-api sqream sqreamdb',
    python_requires = '>=3.7.1'
)

if __name__ == '__main__':
    setup(**setup_params)
