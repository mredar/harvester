"""
Script to run the ingest process.


Usage:
    $ python run_ingest.py user-email url_collection_api
"""
import sys
import os
from email.mime.text import MIMEText
from dplaingestion.scripts import enrich_records
from dplaingestion.scripts import save_records
from dplaingestion.scripts import remove_deleted_records
from dplaingestion.scripts import dashboard_cleanup
from dplaingestion.scripts import check_ingestion_counts
import logbook
from harvester import fetcher
from harvester.parse_env import parse_env
from harvester.collection_registry_client import Collection
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
import rq
from harvester import solr_updater
from harvester import grab_solr_index

EMAIL_RETURN_ADDRESS = os.environ.get('RETURN_EMAIL_ADDRESS', 'example@example.com')

def create_mimetext_msg(mail_from, mail_to, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = str(subject)
    msg['From'] = mail_from
    msg['To'] = mail_to
    return msg

def get_redis_connection(redis_host, redis_port, redis_pswd, redis_timeout=10):
    return Redis(host=redis_host, port=redis_port, password=redis_pswd, socket_connect_timeout=redis_timeout)

def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('url_api_collection', type=str,
            help='URL for the collection Django tastypie api resource')
    return parser

def main(user_email, url_api_collection, log_handler=None,
        mail_handler=None, dir_profile='profiles', profile_path=None,
        config_file='akara.ini', redis_host=None, redis_port=None,
        redis_pswd=None):
    logger = logbook.Logger('run_ingest')
    if not mail_handler:
        mail_handler = logbook.MailHandler(EMAIL_RETURN_ADDRESS, user_email,
                                           level=logbook.ERROR) 
    if not( redis_host and redis_port and redis_pswd):
    	redis_host, redis_port, redis_pswd, redis_connect_timeout, id_ec2_ingest, id_ec2_solr_build = parse_env()
 
    try:
        collection = Collection(url_api_collection)
    except Exception, e:
        mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, user_email,
                        'Collection init failed for ' + url_api_collection,
                        ' '.join(('Exception in Collection', url_api_collection,                        ' init', str(e))))
        mail_handler.deliver(mimetext, user_email)
        raise e
    if not log_handler:
        log_handler = logbook.StderrHandler(level='DEBUG')

    ingest_doc_id, num_recs, dir_save = fetcher.main(
                        user_email,
                        url_api_collection,
                        log_handler=log_handler,
                        mail_handler=mail_handler
            )

    logger.info( "INGEST DOC ID:{0}".format(ingest_doc_id))
    logger.info('HARVESTED {0} RECORDS'.format(num_recs))
    logger.info('IN DIR:{0}'.format(dir_save))
    resp = enrich_records.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error enriching records")
        sys.exit(1)
    logger.info('Enriched records')

    resp = save_records.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error("Error saving records {0}".format(str(resp)))
        sys.exit(1)
    logger.info("SAVED RECS")

    resp = remove_deleted_records.main([None, ingest_doc_id]) 
    if not resp == 0:
        logger.error( "Error deleting records")
        sys.exit(1)

    resp = check_ingestion_counts.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error( "Error checking counts")
        sys.exit(1)

    resp = dashboard_cleanup.main([None, ingest_doc_id])
    if not resp == 0:
        logger.error( "Error cleaning up dashboard")
        sys.exit(1)

    rQ = rq.Queue(connection=get_redis_connection(redis_host, redis_port, redis_pswd))
    update_job = rQ.enqueue(solr_updater.main)
    logger.info("Solr Update queuedd for {0}!".format(url_api_collection))
    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    #fetch_index_job = rQ.enqueue(grab_solr_index.main, depends_on=update_job)

if __name__ == '__main__':
    parser = def_args()
    args = parser.parse_args(sys.argv[1:])
    if not args.user_email or not args.url_api_collection:
        parser.print_help()
        sys.exit(27)
    redis_host, redis_port, redis_pswd, redis_connect_timeout, id_ec2_ingest, id_ec2_solr_build = parse_env()
    print("HOST:{0}  PORT:{1}".format(redis_host, redis_port, ))
    print "EMAIL", args.user_email, " URI: ", args.url_api_collection
    main(args.user_email, args.url_api_collection, redis_host=redis_host,
        redis_port=redis_port, redis_pswd=redis_pswd)
