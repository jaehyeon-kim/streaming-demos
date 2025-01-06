FROM python:3.10-slim

## install dependent packages
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

## create a user
RUN useradd app && mkdir /home/app \
    && chown app:app /home/app

USER app
WORKDIR /home/app
