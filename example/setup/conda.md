# Conda

Conda is a great dependency manager for Python. Here are some instructions on how to setup a conda development environment.

## Quick Start

You need to have python setup, and optionally conda. I highly recommend having conda installed (it makes everything much simpler to keep track of). You can check out how to install miniconda [here](https://docs.conda.io/en/latest/miniconda.html).

Once you finish the installation of miniconda, open a new shell. You should see that your current python environment is now `base` (displayed next to your username in your terminal).

We'll now create a custom python environment for the `tweepipe` module:

```bash
conda env create -f environment.yml
```
> Make sure you are at the root of the tweepipe working directory.

Once the environment is created, add `tweepipe` to your modules as follows:

```bash
conda develop .
```
> Again, make sure you are at the root of the tweepipe working directory.

If you successfully completed these steps, you can skip the `Installation` section below.
If you open a python shell and run the following command, they should execute without any error:

```python
import tweepipe
```