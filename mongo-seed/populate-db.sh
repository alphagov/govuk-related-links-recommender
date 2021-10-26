#!/bin/sh



DB_URL="mongodb://content-store/content_store"

echo "POPULATING CONTENT STORE"

num_records=$(mongosh --quiet --eval "db.content_items.find().count()" ${DB_URL})
echo "$num_records content items found"

if [ "$num_records" -eq 0 ]; then
    echo "populating content_store from dump"
#    tar xzf mongodump.tgz var/lib/mongodb/backup/mongodump/content_store_production/content_items.bson
#    mongorestore --host=content-store -d content_store -c content_items var/lib/mongodb/backup/mongodump/content_store_production/content_items.bson
    mongorestore --host=content-store -d content_store -c content_items /var/tmp/content_items.bson
    num_new_records=$(mongosh --quiet --eval "db.content_items.find().count()" ${DB_URL})
    echo "$num_new_records content items now in the content_store"
else
    echo "No need to populate content_store"
fi
