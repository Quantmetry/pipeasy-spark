# pipeasy-spark


[![pypi badge](https://img.shields.io/pypi/v/pipeasy_spark.svg)](https://pypi.python.org/pypi/pipeasy_spark)

[![](https://img.shields.io/travis/Quantmetry/pipeasy-spark.svg)](https://travis-ci.org/Quantmetry/pipeasy-spark)

[![documentation badge](https://readthedocs.org/projects/pipeasy-spark/badge/?version=latest)](https://readthedocs.org/projects/pipeasy-spark/)

[![pyup badge](https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/shield.svg)](https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/)


an easy way to define preprocessing data pipelines for pysparark


* Free software: MIT license
* Documentation: https://pipeasy-spark.readthedocs.io.


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

Features
--------

* TODO

Credits
-------

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter)
and the [`audreyr/cookiecutter-pypackage`](https://github.com/audreyr/cookiecutter-pypackage) project template.

