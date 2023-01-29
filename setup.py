from setuptools import setup, find_packages

with open("README.rst", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setup_params = dict(
    name =          'pysqream',
    version =       '3.2.3',
    description =   'DB-API connector for SQream DB', 
    long_description = long_description,
    url = "https://github.com/SQream/pysqream",
    author = "SQream",
    author_email = "info@sqream.com",
    packages = find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    keywords = 'database db-api sqream sqreamdb',
    python_requires = '>=3.6.5',
    install_requires=["numpy==1.19.5", "packaging==20.9", "pyarrow==6.0.1",
                      "pytest==6.2.3", "setuptools==57.4.0"]
)

if __name__ == '__main__':
    setup(**setup_params)
