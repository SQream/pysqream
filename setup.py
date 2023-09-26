from setuptools import setup, find_packages

with open("README.rst", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup_params = dict(
    name =          'pysqream',
    version =       '5.0.0',
    description =   'DB-API connector for SQream DB', 
    long_description = long_description,
    url = "https://github.com/SQream/pysqream",
    author = "SQream",
    author_email = "info@sqream.com",
    packages = find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    keywords = 'database db-api sqream sqreamdb',
    python_requires = '>=3.9',
    install_requires=["numpy>=1.20", "packaging>=23.0", "pyarrow>=6.0.1",
                      "setuptools>=57.4.0"]
)

if __name__ == '__main__':
    setup(**setup_params)
