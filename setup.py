import setuptools
from distutils.core import setup

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysqream",
    version="2.1.3a1",
    author="SQream Technologies",
    author_email="info@sqream.com",
    description="Python Native API for communicating with SQream DB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SQream/pysqream",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD3 License",
        "Operating System :: OS Independent",
    ],
    keywords='database sqream sqreamdb',
    python_requires='>=2.7, ~=3.3'
)