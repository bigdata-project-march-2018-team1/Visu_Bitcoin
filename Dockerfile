FROM python:3

COPY ./app/requirements.txt /tmp/.
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN rm /tmp/requirements.txt

RUN echo 'deb http://deb.debian.org/debian jessie-backports main' \
      > /etc/apt/sources.list.d/jessie-backports.list
RUN apt-get update
RUN apt-get install --target-release jessie-backports openjdk-8-jre-headless ca-certificates-java --assume-yes
WORKDIR /app
CMD spark-submit src/__main__.py
