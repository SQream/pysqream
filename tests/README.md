## Running the tests

### Create virtual environment

```commandline
python3 -m venv venv
```

### Activate virtual environment

```commandline
. ./venv/bin/activate
```

### Run pytest
`pytest -v`

## Checking test coverage
Using `coverage` package:

`pip3 install coverage`
 
To run the test from the test folder, saving the html report in a folder `test_report`:

`coverage run pysqream_tests.py  && coverage html --include='../pysqream/dbapi.py' -d test_report`

To see the report with your web browser:

`firefox test_report/index.html`

## Checking for uncovered code in new commit / branch
Using `diff-cover` package:

`pip3 install diff_cover`

Run the coverage test on your code and generate an xml report

`coverage run pysqream_tests.py  && coverage xml`

Then generate html of new uncovered code:

`diff-cover coverage.xml --html-report commit_coverage.html && firefox commit_coverage.html`

To know if any code is uncovered in the new commit after running the above:

`python3 is_commit_covered.py`
