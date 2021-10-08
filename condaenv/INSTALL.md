# Setup development environment

Create my-base virtual environemnt

    conda env create -f env-my-base.yaml

Create mrtools-dev virtual environment

    conda activate my-base
    mamba create -y -n mrtools-dev -c conda-forge --file=pkgs-dev.txt
    conda deactivate my-base

Install MRTools

    conda activate mrtools-dev
    poetry install 
