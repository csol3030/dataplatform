FROM quay.io/astronomer/astro-runtime:7.3.0

##### Docker Customizations below this line #####
## Install Azure CLI - Direct
USER root
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# USER ASTRO
# RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# The directory where `pyenv` will be installed. You can update the path as needed
ENV PYENV_ROOT="/home/astro/.pyenv" 
ENV PATH=${PYENV_ROOT}/bin:${PATH}

## If you want to check your dependency conflicts for extra packages that you may require for your ## venv, uncomment the following two lines to install pip-tools
# RUN pip-compile -h
# RUN pip-compile snowpark_requirements.txt

# Install the required version of pyenv and create the virtual environment
RUN curl https://pyenv.run | bash  && \
    eval "$(pyenv init -)" && \
    pyenv install 3.8.14 && \
    pyenv virtualenv 3.8.14 snowpark_env && \
    pyenv activate snowpark_env && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r snowpark_requirements.txt


