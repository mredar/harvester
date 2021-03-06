#! /bin/env python
import sys
import os
from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.image_harvest import harvest_image_for_doc

EMAIL_RETURN_ADDRESS = os.environ.get('EMAIL_RETURN_ADDRESS',
                                      'example@example.com')
# csv delim email addresses
EMAIL_SYS_ADMIN = os.environ.get('EMAIL_SYS_ADMINS', None)
IMAGE_HARVEST_TIMEOUT = 144000

def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('rq_queue', type=str, help='RQ Queue to put job in')
    parser.add_argument('doc_id_list_file', type=str,
            help='File containing couchdb doc ids to process')
    parser.add_argument('--object_auth', nargs='?',
            help='HTTP Auth needed to download images - username:password')
    parser.add_argument('--url_couchdb', nargs='?',
            help='Override url to couchdb')
    parser.add_argument('--timeout', nargs='?',
            help='set image harvest timeout in sec (14400 - 4hrs default)')
    parser.add_argument('--get_if_object', action='store_true',
                        default=False,
            help='Should image harvester not get image if the object field exists for the doc (default: False, always get)')
    return parser

def main(user_email, doc_id_list_file, rq_queue=None, url_couchdb=None):
    enq = CouchDBJobEnqueue(rq_queue=rq_queue)
    timeout = 10000000
    with open(doc_id_list_file) as foo:
        doc_id_list = [ l.strip() for l in foo.readlines()]
    results = enq.queue_list_of_ids(doc_id_list,
                     timeout,
                     harvest_image_for_doc,
                     url_couchdb=url_couchdb,
                     )
    print "Results:{}".format(results)

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.doc_id_list_file:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    if args.object_auth:
        kwargs['object_auth'] = args.object_auth
    if args.timeout:
        kwargs['harvest_timeout'] = int(args.timeout)
    if args.get_if_object:
        kwargs['get_if_object'] = args.get_if_object

    main(args.user_email,
            args.doc_id_list_file,
            rq_queue = args.rq_queue,
            **kwargs)


