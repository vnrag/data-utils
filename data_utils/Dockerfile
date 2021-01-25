FROM python:3.6

Copy ./Pipfile /tmp

RUN pip install --upgrade pip \
&& pip install pipenv \
&& cd /tmp \
&& pipenv lock --requirements > requirements.txt \
&& pip install -r requirements.txt \
&& pip install pytest pytest-cov pytest-pep8 pytest-freezegun moto mocksftp autopep8 pep8 \
&& rm -rf /tmp/*

COPY . /app
WORKDIR /app/src/
ENV PYTHONPATH=/app/src/
ENV PYTHONPATH "${PYTHONPATH}":/app/src/utils/
ENV PYTHONPATH "${PYTHONPATH}":/app/src/config/
ENV AWS_DEFAULT_REGION=eu-central-1

RUN cd /app && py.test -v test/
