#! /bin/env python
""""
Usage:
    $ python queue_image_harvest.py user-email url_collection_api
"""
import sys
import os
from harvester.config import config as config_harvest
from harvester.collection_registry_client import Collection
import logbook
from redis import Redis
from rq import Queue

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
    parser.add_argument(
        'url_api_collection',
        type=str,
        help='URL for the collection Django tastypie api resource')
    parser.add_argument(
        '--object_auth',
        nargs='?',
        help='HTTP Auth needed to download images - username:password')
    parser.add_argument(
        '--url_couchdb', nargs='?', help='Override url to couchdb')
    parser.add_argument(
        '--timeout',
        nargs='?',
        help='set image harvest timeout in sec (14400 - 4hrs default)')
    parser.add_argument(
        '--get_if_object',
        action='store_true',
        default=False,
        help='Should image harvester not get image if the object '
        'field exists for the doc (default: False, always get)'
    )
    return parser


def queue_image_harvest(redis_host,
                        redis_port,
                        redis_password,
                        redis_timeout,
                        rq_queue,
                        collection_key,
                        url_couchdb=None,
                        object_auth=None,
                        get_if_object=False,
                        harvest_timeout=IMAGE_HARVEST_TIMEOUT):
    rQ = Queue(
        rq_queue,
        connection=Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            socket_connect_timeout=redis_timeout))
    job = rQ.enqueue_call(
        func='harvester.image_harvest.main',
        kwargs=dict(
            collection_key=collection_key,
            url_couchdb=url_couchdb,
            object_auth=object_auth,
            get_if_object=get_if_object),
        timeout=harvest_timeout)
    return job


def main(user_email,
         url_api_collections,
         log_handler=None,
         mail_handler=None,
         dir_profile='profiles',
         profile_path=None,
         config_file='akara.ini',
         rq_queue=None,
         **kwargs):
    '''Runs a UCLDC ingest process for the given collection'''
    emails = [user_email]
    if EMAIL_SYS_ADMIN:
        emails.extend([u for u in EMAIL_SYS_ADMIN.split(',')])
    if not mail_handler:
        mail_handler = logbook.MailHandler(
            EMAIL_RETURN_ADDRESS, emails, level='ERROR', bubble=True)
    mail_handler.push_application()
    config = config_harvest(config_file=config_file)
    if not log_handler:
        log_handler = logbook.StderrHandler(level='DEBUG')
    log_handler.push_application()

    for url_api_collection in [x for x in url_api_collections.split(';')]:
        try:
            collection = Collection(url_api_collection)
        except Exception, e:
            msg = 'Exception in Collection {}, init {}'.format(
                    url_api_collection,
                    str(e))
            logbook.error(msg)
            raise e
        queue_image_harvest(
            config['redis_host'],
            config['redis_port'],
            config['redis_password'],
            config['redis_connect_timeout'],
            rq_queue=rq_queue,
            collection_key=collection.id,
            object_auth=collection.auth,
            **kwargs)

    log_handler.pop_application()
    mail_handler.pop_application()


if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection:
        parser.print_help()
        sys.exit(27)
    kwargs = {}
    if args.object_auth:
        kwargs['object_auth'] = args.object_auth
    if args.timeout:
        kwargs['harvest_timeout'] = int(args.timeout)
    if args.get_if_object:
        kwargs['get_if_object'] = args.get_if_object
    main(
        args.user_email,
        args.url_api_collection,
        rq_queue=args.rq_queue,
        **kwargs)
