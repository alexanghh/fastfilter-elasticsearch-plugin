FROM docker.elastic.co/elasticsearch/elasticsearch:8.5.0
ADD build/distributions/fastfilter-elasticsearch-plugin-8.5.0.zip /tmp/
RUN bin/elasticsearch-plugin install file:///tmp/fastfilter-elasticsearch-plugin-8.5.0.zip
LABEL authors="alexanghh"
