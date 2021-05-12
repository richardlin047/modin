#!/bin/sh

USERNAME=ubuntu
CONDA_ENV=py37_tensorflow
sudo -u "$USERNAME" -i /bin/bash -l -c "conda init bash"
sudo -u "$USERNAME" -i /bin/bash -l -c "conda activate $CONDA_ENV;"

sudo -u "$USERNAME" -i /bin/bash -l -c "git clone https://github.com/richardlin047/modin.git; cd modin; git pull origin partition_benchmark; python scaleup_dataframe.py"