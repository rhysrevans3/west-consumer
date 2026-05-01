FROM public.ecr.aws/sam/build-python3.12:latest-x86_64

ENV LD_LIBRARY_PATH=/lib:/usr/lib:/usr/local/lib
ENV PATH=/root/.local/bin:/sbin:/usr/sbin:${PATH}

RUN dnf upgrade -y
RUN dnf group install -y "Development Tools"
RUN dnf install -y gcc git libcurl-devel make openssl openssl-devel which

RUN git clone https://github.com/confluentinc/librdkafka  && \
    cd librdkafka && git checkout tags/v2.6.0 && \
    ./configure --install-deps && make && make install && \
    ldconfig

# Install poetry
RUN pip install poetry

WORKDIR $PYSETUP_PATH

ENV PATH="$POETRY_HOME/bin:$PATH"

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --extras globus

COPY ./src/west_consumer .

CMD ["python", "main.py"]