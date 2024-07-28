FROM docker.elastic.co/elasticsearch/elasticsearch:8.14.3
ADD build/distributions/fastfilter-elasticsearch-plugin-8.14.3.zip /tmp/
RUN bin/elasticsearch-plugin install file:///tmp/fastfilter-elasticsearch-plugin-8.14.3.zip
LABEL authors="alexanghh"
