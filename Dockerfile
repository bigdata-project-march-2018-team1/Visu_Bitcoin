FROM python:3

COPY ./app/requirements.txt /tmp/.
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN rm /tmp/requirements.txt

RUN apt-get update

WORKDIR /app
CMD ["python", "src"]
