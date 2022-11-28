import os

from setuptools import find_packages, setup

import tweepipe


with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="tweepipe",
    description=".",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=tweepipe.__version__,
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[],
    license="MIT",
)
