#!/usr/bin/env python

from setuptools import setup, find_packages
setup(
    name = "HelloWorld",
    version = "0.1",
    packages = find_packages(exclude=['tests', 'output']),
    scripts = ['say_hello.py'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = ['pysftp>=0.2.8',
                        'paramiko>=1.13.0',
                        'boto>=2.32.1',
                        'web.py>=0.37'],

    package_data = {
        # If any package contains *.txt or *.md files, include them:
        '': ['*.md', '*.txt'],
    },

    # metadata for upload to PyPI
    author = "Jay Bhambhani",
    author_email = "jay@cinchfinancial.com",
    description = "Prototype of Optimal Blue Harvester",
    license = "BSD",
    keywords = "cinch harvester etl",
    

    # could also include long_description, download_url, classifiers, etc.
)