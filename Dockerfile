FROM docker.elastic.co/elasticsearch/elasticsearch:8.14.3
ADD build/distributions/fastfilter-elasticsearch-plugin-1.0.0-SNAPSHOT.zip /tmp/
RUN bin/elasticsearch-plugin install file:///tmp/fastfilter-elasticsearch-plugin-1.0.0-SNAPSHOT.zip
LABEL authors="alexanghh"
