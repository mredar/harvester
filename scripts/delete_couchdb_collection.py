import sys
import argparse
import os
from harvester.couchdb_init import get_couchdb
import couchdb
from harvester.post_processing.couchdb_runner import CouchDBCollectionFilter

COUCHDB_VIEW = 'all_provider_docs/by_provider_name'

def confirm_deletion(cid):
    prompt = "Are you sure you want to delete all couchdb " + \
             "documents for %s? yes to confirm\n" % cid
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        else:
            return False

def delete_collection(cid):
    _couchdb = get_couchdb()
    rows = CouchDBCollectionFilter(collection_key=cid,
                                        couchdb_obj=_couchdb
                                        )
    deleted = []
    num_deleted = 0
    for row in rows:
        doc = _couchdb.get(row['id'])
        deleted.append(row['id'])
        _couchdb.delete(doc)
        print "DELETED: {0}".format(row['id'])
        num_deleted +=1
    return num_deleted, deleted 

if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description='Delete all documents in given collection')
    parser.add_argument('collection_id',
                        help='Registry id for the collection')
    parser.add_argument('--yes', action='store_true',
                     help="Don't prompt for deletion, just do it")
    args = parser.parse_args(sys.argv[1:])
    if args.yes or confirm_deletion(args.collection_id):
        print 'DELETING COLLECTION {}'.format(args.collection_id)
        num, deleted_ids = delete_collection(args.collection_id)
        print "DELTED {} DOCS".format(num)
        print "DELTED {} DOCS".format(num)
    else:
        print "Exiting without deleting"
