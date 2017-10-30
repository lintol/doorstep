FROM python:3.6

COPY requirements.txt /

RUN pip3 install -r /requirements.txt

COPY . /doorstep

WORKDIR /doorstep

RUN python3 setup.py install
