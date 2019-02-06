# pipeasy-spark


[![pypi badge](https://img.shields.io/pypi/v/pipeasy_spark.svg)](https://pypi.python.org/pypi/pipeasy_spark)
[![](https://img.shields.io/travis/Quantmetry/pipeasy-spark.svg)](https://travis-ci.org/Quantmetry/pipeasy-spark)
[![documentation badge](https://readthedocs.org/projects/pipeasy-spark/badge/?version=latest)](https://readthedocs.org/projects/pipeasy-spark/)
[![pyup badge](https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/shield.svg)](https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/)


`pipeasy-spark` provides an easy way to define a preprocessing data [pipeline for pyspark](https://spark.apache.org/docs/latest/ml-pipeline.html).

This is similar to what [DataframeMapper](https://github.com/scikit-learn-contrib/sklearn-pandas#transformation-mapping)
or [ColumnTransformer](https://scikit-learn.org/stable/modules/generated/sklearn.compose.ColumnTransformer.html)
do for `scikit-learn`.


* Free software: MIT license
* Documentation: https://pipeasy-spark.readthedocs.io.

Verified compatibility:
- python 3.5, 3.6
- pyspark 2.1 -> 2.4

# Documentation

https://pipeasy-spark.readthedocs.io/en/latest/

# Installation


```
pip install pipeasy-spark
```

# Usage

The goal of this package is to easily create a `pyspark.ml.Pipeline` instance that can
transform the columns of a `pyspark.sql.Dataframe` in order to prepare it for a regressor or classifier.


Assuming we have the `titanic` dataset as a Dataframe:

```python
df = titanic.select('Survived', 'Sex', 'Age').dropna()
df.show(2)
# +--------+------+----+
# |Survived|   Sex| Age|
# +--------+------+----+
# |       0|  male|22.0|
# |       1|female|38.0|
# +--------+------+----+
```

A basic transformation pipeline can be created as follows. We define for each
column of the dataframe a list of transformers that are applied sequencially.
Each transformer is an instance of a transformer from `pyspark.ml.feature`.
Notice that we do not provide the parameters `inputCol` or `outputCol` to
these transformers.

```python
from pipeasy_spark import build_pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoderEstimator,
    VectorAssembler,
    StandardScaler,
)

pipeline = build_pipeline(column_transformers={
    # 'Survived' : this variable is not modified, it can also be omitted from the dict
    'Survived': [],
    'Sex': [StringIndexer(), OneHotEncoderEstimator(dropLast=False)],
    # 'Age': a VectorAssembler must be applied before the StandardScaler
    # as the latter only accepts vectors as input.
    'Age': [VectorAssembler(), StandardScaler()]
})
trained_pipeline = pipeline.fit(df)
trained_pipeline.transform(df).show(2)
# +--------+-------------+--------------------+
# |Survived|          Sex|                 Age|
# +--------+-------------+--------------------+
# |       0|(2,[0],[1.0])|[1.5054181442954726]|
# |       1|(2,[1],[1.0])| [2.600267703783089]|
# +--------+-------------+--------------------+
```

This preprocessing pipeline can be included in a full prediction pipeline :

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression

full_pipeline = Pipeline(stages=[
    pipeline,
    # all the features have to be assembled in a single column:
    VectorAssembler(inputCols=['Sex', 'Age'], outputCol='features'),
    LogisticRegression(featuresCol='features', labelCol='Survived')
])

trained_predictor = full_pipeline.fit(df)
trained_predictor.transform(df).show(2)
# +--------+-------------+--------------------+--------------------+--------------------+--------------------+----------+
# |Survived|          Sex|                 Age|            features|       rawPrediction|         probability|prediction|
# +--------+-------------+--------------------+--------------------+--------------------+--------------------+----------+
# |       0|(2,[0],[1.0])|[1.5054181442954726]|[1.0,0.0,1.505418...|[2.03811507112527...|[0.88474119316485...|       0.0|
# |       1|(2,[1],[1.0])| [2.600267703783089]|[0.0,1.0,2.600267...|[-0.7360149659890...|[0.32387617489886...|       1.0|
# +--------+-------------+--------------------+--------------------+--------------------+--------------------+----------+
```

As of this writing, these are the transformers from `pyspark.ml.feature` that are supported:

```python
[
    Binarizer(threshold=0.0, inputCol=None, outputCol=None),
    BucketedRandomProjectionLSH(inputCol=None, outputCol=None, seed=None, numHashTables=1, bucketLength=None),
    Bucketizer(splits=None, inputCol=None, outputCol=None, handleInvalid='error'),
    CountVectorizer(minTF=1.0, minDF=1.0, vocabSize=262144, binary=False, inputCol=None, outputCol=None),
    DCT(inverse=False, inputCol=None, outputCol=None),
    ElementwiseProduct(scalingVec=None, inputCol=None, outputCol=None),
    FeatureHasher(numFeatures=262144, inputCols=None, outputCol=None, categoricalCols=None),
    HashingTF(numFeatures=262144, binary=False, inputCol=None, outputCol=None),
    IDF(minDocFreq=0, inputCol=None, outputCol=None),
    Imputer(strategy='mean', missingValue=nan, inputCols=None, outputCols=None),
    IndexToString(inputCol=None, outputCol=None, labels=None),
    MaxAbsScaler(inputCol=None, outputCol=None),
    MinHashLSH(inputCol=None, outputCol=None, seed=None, numHashTables=1),
    MinMaxScaler(min=0.0, max=1.0, inputCol=None, outputCol=None),
    NGram(n=2, inputCol=None, outputCol=None),
    Normalizer(p=2.0, inputCol=None, outputCol=None),
    OneHotEncoder(dropLast=True, inputCol=None, outputCol=None),
    OneHotEncoderEstimator(inputCols=None, outputCols=None, handleInvalid='error', dropLast=True),
    PCA(k=None, inputCol=None, outputCol=None),
    PolynomialExpansion(degree=2, inputCol=None, outputCol=None),
    QuantileDiscretizer(numBuckets=2, inputCol=None, outputCol=None, relativeError=0.001, handleInvalid='error'),
    RegexTokenizer(minTokenLength=1, gaps=True, pattern='\\\\s+', inputCol=None, outputCol=None, toLowercase=True),
    StandardScaler(withMean=False, withStd=True, inputCol=None, outputCol=None),
    StopWordsRemover(inputCol=None, outputCol=None, stopWords=None, caseSensitive=False),
    StringIndexer(inputCol=None, outputCol=None, handleInvalid='error', stringOrderType='frequencyDesc'),
    Tokenizer(inputCol=None, outputCol=None),
    VectorAssembler(inputCols=None, outputCol=None),
    VectorIndexer(maxCategories=20, inputCol=None, outputCol=None, handleInvalid='error'),
    VectorSizeHint(inputCol=None, size=None, handleInvalid='error'),
    VectorSlicer(inputCol=None, outputCol=None, indices=None, names=None),
    Word2Vec(vectorSize=100, minCount=5, numPartitions=1, stepSize=0.025, maxIter=1, seed=None, inputCol=None, outputCol=None, windowSize=5, maxSentenceLength=1000)
]
```

These are not supported as it is not possible to specity the input column(s).

```python
[
    ChiSqSelector(numTopFeatures=50, featuresCol='features', outputCol=None, labelCol='label', selectorType='numTopFeatures', percentile=0.1, fpr=0.05, fdr=0.05, fwe=0.05),
    LSHParams(),
    RFormula(formula=None, featuresCol='features', labelCol='label', forceIndexLabel=False, stringIndexerOrderType='frequencyDesc', handleInvalid='error'),
    SQLTransformer(statement=None)
]
```


# Contributing

In order to contribute to the project, here is how you should configure your local development environment:

- download the code and create a local virtual environment with the required dependencies:

    ```
    # getting the project
    $ git clone git@github.com:Quantmetry/pipeasy-spark.git
    $ cd pipeasy-spark
    # creating the virtual environnment and activating it
    $ make install_dev
    $ source .venv/bin/activate
    (.venv) $ make test
    # start the demo notebook
    (.venv) $ make notebook
    ```

    **Note:** the `make install` step installs the package in *editable* mode into the local virtual environment.
    This step is required as it also installs the package dependencies as they are listed in the `setup.py` file.

- make sure that Java is correcly installed. [Download](https://www.java.com/en/download/mac_download.jsp)
  Java and install it. Then you should set the `JAVA_HOME` environment variable. For instance you can add the
  following line to your `~/.bash_profile`:

  ```
  # ~/.bash_profile   -> this depends on your platform
  # note that the actual path might change for you
  export JAVA_HOME="/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home/"
  ```

  **Note:** notice that we did not install spark itself. Having a valid Java Runtime Engine installed and installing
  `pyspark` (done when installing the package's dependencies) is enough to run the tests.

- run the tests of the project (you need to activate your local virtual environnment):

    ```
    $ source .venv/bin/activate
    # this runs the tests/ with the current python version
    (.venv) $ make test
    # check that the developped code follows the standards
    # (we use flake8 as a linting engine, it is configured in the tox.ini file)
    (.venv) $ make lint
    # run the tests for several python versions + run linting step
    (.venv) $ tox
    ```

    **Note:** the continuous integration process (run by TravisCI) performs the latest operation (`$ tox`). Consequently you should make sure that this step is successfull on your machine before pushing new code to the repository. However you might not have all python versions installed on your local machine; this is ok in most cases.

- (optional) configure your text editor. Because the tests include a linting step, it is convenient to add this linting to your
  editor. For instance you can use VSCode with the Python extension and add linting with flake8 in the settings.
  It is a good idea to use as a python interpreter for linting (and code completetion etc.) the one in your local virtuel environnment.
  flake8 configuration options are specified in the `tox.ini` file.


# Notes on setting up the project

- I setup the project using [this cookiecutter project](https://cookiecutter-pypackage.readthedocs.io/en/latest/readme.html#features)
- create a local virtual environnment and activate it
- install requirements_dev.txt

  ```
  $ python3 -m venv .venv
  $ source .venv/bin./activate
  (.venv) $ pip install -r requirements_dev.txt
  ```

- I update my vscode config to use this virtual env as the python interpreter for this project ([doc](https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter))
  (the modified file is  <my_repo>/.vscode/settings.json)
- I update the `Makefile` to be able to run tests (this way I don't have to mess with the `PYTHONPATH`):

  ```
  # avant
  test: ## run tests quickly with the default Python
  py.test

  # modification
  test: ## run tests quickly with the default Python
  python -m pytest tests/
  ```

  I can now run (when inside my local virtual environment):

  ```
  (.venv) $ make test
  ```

  I can also run `tox` which runs the tests agains several python versions:

  ```
  (.venv) $ tox
  py27: commands succeeded
  ERROR:   py34: InterpreterNotFound: python3.4
  ERROR:   py35: InterpreterNotFound: python3.5
  py36: commands succeeded
  flake8: commands succeeded
  ```

- I log into Travis with my Github account. I can see and configure the builds for this repository (I have admin rights on the repo).
  I can trigger a build without pushing to the repository (More Options / Trigger Build). Everything runs fine!
- I push this to a new branch : Travis triggers tests on this branch (even without creating a pull request).
  The tests fail because I changed `README.rst` to `README.md`. I need to also change this in `setup.py`.
- I create an account on pypi.org and link it to the current project
  ([documentation](https://cookiecutter-pypackage.readthedocs.io/en/latest/travis_pypi_setup.html#travis-pypi-setup))

  ```
  $ brew install travis
  $ travis encrypt ****** --add deploy.password
  ```

  This modifies the `.travis.yml` file. I customize it slightly because the package name is wrong:

  ```
  # .travis.yml
  deploy:
    on:
    tags: true
    # the repo was not correct:
    repo: Quantmetry/pipeasy-spark
  ```

- I update the version and push a tag:

  ```
  $ bumpversion patch
  $ git push --tags
  $ git push
  ```

  I can indeed see the tag (and an associated release) on the github interface. However Travis does not deploy
   on this commit. This is the intended behaviour. Be [default](https://docs.travis-ci.com/user/deployment/pypi/)
   travis deploys only on the `master` branch.

- I setup an account on [readthedoc.io](https://readthedocs.org/). Selecting the repo is enough to have the documentation online! 

- When I merge to master Travis launches a build bus says it will not deploy
  (see [this build](https://travis-ci.org/Quantmetry/pipeasy-spark/jobs/440637481) for instance). However the library
  was indeed deployed to pypi: I can pip install it..

Note: when Omar pushed new commits, travis does not report their status.

- I update `setup.py` to add the dependence to `pyspark`. I also modify slightly the development setup (see Development section above).
