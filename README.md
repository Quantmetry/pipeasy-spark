# pipeasy-spark


[![pypi badge](https://img.shields.io/pypi/v/pipeasy_spark.svg)](https://pypi.python.org/pypi/pipeasy_spark)

[![](https://img.shields.io/travis/Quantmetry/pipeasy-spark.svg)](https://travis-ci.org/Quantmetry/pipeasy-spark)

[![documentation badge](https://readthedocs.org/projects/pipeasy-spark/badge/?version=latest)](https://readthedocs.org/projects/pipeasy-spark/)

[![pyup badge](https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/shield.svg)](https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/)


an easy way to define preprocessing data pipelines for pysparark


* Free software: MIT license
* Documentation: https://pipeasy-spark.readthedocs.io.


# Development environnment

In order to contribute to the project, here is how you should configure your local development environment:

- download the code and create a local virtual environment with the required dependencies:

    ```
    # getting the project
    $ git clone git@github.com:Quantmetry/pipeasy-spark.git
    $ cd pipeasy-spark
    # creating the virtual environnment and activating it
    $ python3 -m venv .venv
    $ source .venv/bin./activate
    # installing the developpment dependencies (pytest, flake8, etc.)
    (.venv) $ pip install -r requirements_dev.txt
    # installing the project dependencies
    (.venv) $ make install
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


Notes on setting up the project
-------------------------------

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

Features
--------

* TODO

Credits
-------

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter)
and the [`audreyr/cookiecutter-pypackage`](https://github.com/audreyr/cookiecutter-pypackage) project template.

