FROM mongo:4.0

#COPY mongodump.tgz .
#RUN tar xzf mongodump.tgz var/lib/mongodb/backup/mongodump/content_store_production/content_items.bson
#CMD mongorestore --host=content-store -d content_store -c content_items var/lib/mongodb/backup/mongodump/content_store_production/content_items.bson

WORKDIR /root
COPY content_store_production .

CMD mongorestore --host=content-store -d content_store -c content_items content_store/content_items.bson
