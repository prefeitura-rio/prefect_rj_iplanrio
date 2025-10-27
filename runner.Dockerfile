FROM ghcr.io/actions/actions-runner:latest

ENV PYTHON_VERSION=3.13.7

# installing global dependecies
RUN sudo apt update \
   && sudo apt install -y curl wget build-essential libncursesw5-dev libssl-dev \
     libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev \
     zlib1g-dev git

# installing python
RUN curl https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz -L -o /tmp/Python-$PYTHON_VERSION.tgz \
    && tar xzf /tmp/Python-$PYTHON_VERSION.tgz -C /tmp \
    && ls -lha /tmp \
    && cd /tmp/Python-$PYTHON_VERSION \
    && ./configure --enable-optimizations --with-ensurepip=install \
    && make -j$(nproc) \
    && sudo make altinstall \
    && sudo rm -r /tmp/Python-$PYTHON_VERSION* \
    && sudo ln -s /usr/local/bin/python3.13 /usr/local/bin/python \
    && sudo ln -s /usr/local/bin/pip3.13 /usr/local/bin/pip

ENV PATH=$PATH:/home/runner/.local/bin

# installing nodejs
RUN sudo apt install nodejs -y

# installing yq
RUN sudo apt install jq -y
RUN pip install yq

# cleaning cache
RUN sudo rm -rf /var/cache/*
