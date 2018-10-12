=============
pipeasy-spark
=============


.. image:: https://img.shields.io/pypi/v/pipeasy_spark.svg
        :target: https://pypi.python.org/pypi/pipeasy_spark

.. image:: https://img.shields.io/travis/BenjaminHabert/pipeasy_spark.svg
        :target: https://travis-ci.org/BenjaminHabert/pipeasy_spark

.. image:: https://readthedocs.org/projects/pipeasy-spark/badge/?version=latest
        :target: https://pipeasy-spark.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/shield.svg
     :target: https://pyup.io/repos/github/BenjaminHabert/pipeasy_spark/
     :alt: Updates



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
        # avant:
        test: ## run tests quickly with the default Python
	py.test

        # modification:
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
 - I create an account on pypi.org and link it to the current project ([documentation](https://cookiecutter-pypackage.readthedocs.io/en/latest/travis_pypi_setup.html#travis-pypi-setup))

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


Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
