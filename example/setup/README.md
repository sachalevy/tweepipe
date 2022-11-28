# How To Setup

These few instructions are to help with setting up a development environment, conda, mongodb, etc.

## Initial Setup

Clone the repository and create a new conda environment containing all dependencies:
*yet to be implemented*

```bash
git clone path_to_git_repository
cd tweepipe/

# Create the Conda environment called 'htpipeline'
conda env create -n tweepipe -f environment.yml

# Activate the environment
conda activate tweepipe

# Set the project root to python path (development mode)
conda develop -n tweepipe .
```

You can also update the environment dependencies by runing `conda env update -n tweepipe -f environment.yml`.

You will need a `.env` file at the root of this repository containing all necessary environment variables. Ask a maintainer of this project for this file.
