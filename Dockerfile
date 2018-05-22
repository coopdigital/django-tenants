FROM ubuntu:trusty

RUN apt-get update && apt-get -y upgrade

RUN apt-get -y install python3-pip python3-psycopg2 git libpq-dev

WORKDIR /code

ADD . /code

RUN python3 setup.py develop
