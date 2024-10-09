How to contribute to the Snuba SnQL SDK
==========================================

``snuba-sdk`` is an ordinary Python package. You can install it with ``pip
install -e .`` into some virtualenv, edit the sourcecode and test out your
changes manually.

Community
--------------

The public-facing channels for support and development of Sentry SDKs can be found on `Discord <https://discord.gg/Ww9hbqr>`_.

Running tests and linters
----------------------------

Make sure you have ``virtualenv`` installed, and the Python versions you care
about. You should have the latest Python 3 installed.

We have a ``Makefile`` that is supposed to help people get started with hacking
on the SDK without having to know or understand the Python ecosystem. You don't
need to ``workon`` or ``bin/activate`` anything, the ``Makefile`` will do everything
for you. Run ``make`` or ``make help`` to list commands.

Of course you can always run the underlying commands yourself, which is
particularly useful when wanting to provide arguments to ``pytest`` to run
specific tests. If you want to do that, we expect you to know your way around
Python development, and you can run the following to get started with ``pytest``:

.. code-block:: python

    # This is "advanced mode". Use `make help` if you have no clue what's
    # happening here!

    pip install -e .
    pip install -r test-requirements.txt

    pytest tests/

Releasing a new version
----------------------------

We use `craft <https://github.com/getsentry/craft#python-package-index-pypi>`_ under the
hood to release new versions.

1. Run the `releases` workflow in Github and input the new version you want to release
2. This creates a PR in the `publish <https://github.com/getsentry/publish>`_ repo, under the issues tab
3. Wait for the PR to have the `accepted` label added to it (this should be done automatically)
4. Once that's done, the new library should be added automatically to the `pypi <https://github.com/getsentry/pypi>`_ repo. It's possible that this step can take a while. Feel free to check on https://github.com/getsentry/pypi/pulls

After this the release should be available for Sentry and also external sources.
