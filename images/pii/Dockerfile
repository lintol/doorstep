FROM lintol/doorstep:latest

# Based on MAINTAINER Ovidiu Dan <contact@ovidiudan.com>
USER root

RUN wget -O ner.zip http://nlp.stanford.edu/software/stanford-corenlp-full-2015-12-09.zip

RUN echo "deb http://deb.debian.org/debian jessie-backports main" >> /etc/apt/sources.list

RUN apt-get update
RUN apt-get -y install -t jessie-backports \
  openjdk-8-jdk \
  openjdk-8-jdk-headless \
  openjdk-8-jre \
  openjdk-8-jre-headless \
  ca-certificates-java

RUN apt-get -y install \
  unzip \
  wget

RUN unzip ner.zip -d /ner

ENV port=9000 outputFormat=inlineXML CLASSPATH=/ner/stanford-corenlp-full-2015-12-09 STANFORD_MODELS=/ner/stanford-corenlp-full-2015-12-09

RUN pip3 install fuzzywuzzy nltk

USER doorstep

RUN python3 -m nltk.downloader 'punkt'
