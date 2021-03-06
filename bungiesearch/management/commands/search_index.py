from collections import defaultdict

from django.core.management.base import BaseCommand
from django.conf import settings
from six import iteritems

from ... import Bungiesearch
from ...logger import logger
from ...utils import update_index


class Command(BaseCommand):
    args = ''
    help = 'Manage search index.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--create',
            action='store_const',
            dest='action',
            const='create',
            help='Create the index specified in the settings with the mapping generating from the search indices.'
        )
        parser.add_argument(
            '--update',
            action='store_const',
            dest='action',
            const='update',
            help='Update the index specified in the settings with the mapping generating from the search indices.')
        parser.add_argument(
            '--update-mapping',
            action='store_const',
            dest='action',
            const='update-mapping',
            help='Update the mapping of specified models (or all models) on the index specified in the settings.')
        parser.add_argument(
            '--delete',
            action='store_const',
            dest='action',
            const='delete',
            help='Delete the index specified in the settings. Requires the "--guilty-as-charged" flag.')
        parser.add_argument(
            '--delete-mapping',
            action='store_const',
            dest='action',
            const='delete-mapping',
            help='Delete the mapping of specified models (or all models) on the index specified in the settings. Requires the "--guilty-as-charged" flag.')
        parser.add_argument(
            '--guilty-as-charged',
            action='store_true',
            dest='confirmed',
            default=False,
            help='Flag needed to delete an index.')
        parser.add_argument(
            '--models',
            action='store',
            dest='models',
            default=None,
            help='Models to be updated, separated by commas. If none are specified, then all models defined in the index will be updated.')
        parser.add_argument(
            '--index',
            action='store',
            dest='index',
            default=None,
            help='Specify the index for which to apply the action, as defined in BUNGIESEARCH.INDEXES of settings. Defaults to using all indices.')
        parser.add_argument(
            '--bulk-size',
            action='store',
            dest='bulk_size',
            default=100,
            type=int,
            help='Specify the number of items to be updated together.')
        parser.add_argument(
            '--num-docs',
            action='store',
            dest='num_docs',
            default=-1,
            type=int,
            help='Specify the maximum number of items to be indexed. By default will index the whole model.')
        parser.add_argument(
            '--start',
            action='store',
            dest='start_date',
            default=None,
            type=str,
            help='Specify the start date and time of documents to be indexed.')
        parser.add_argument(
            '--end',
            action='store',
            dest='end_date',
            default=None,
            type=str,
            help='Specify the end date and time of documents to be indexed.')
        parser.add_argument(
            '--timeout',
            action='store',
            dest='timeout',
            default=None,
            type=int,
            help='Specify the timeout in seconds for each operation.')

    def handle(self, *args, **options):
        src = Bungiesearch(timeout=options.get('timeout'))
        es = src.get_es_instance()
        wait_for_status = settings.BUNGIESEARCH.get('ES_SETTINGS', {}).get('wait_for_status', 'green')

        if not options['action']:
            raise ValueError('No action specified. Must be one of "create", "update" or "delete".')

        if options['action'].startswith('delete'):
            if not options['confirmed']:
                raise ValueError('If you know what a delete operation does (on index or mapping), add the --guilty-as-charged flag.')
            if options['action'] == 'delete':
                if options['index']:
                    indices = [options['index']]
                else:
                    indices = src.get_indices()

                for index in indices:
                    logger.warning('Deleting elastic search index {}.'.format(index))
                    es.indices.delete(index=index, ignore=404)

            else:
                index_to_doctypes = defaultdict(list)
                if options['models']:
                    logger.info('Deleting mapping for models {} on index {}.'.format(options['models'], index))
                    for model_name in options['models'].split():
                        for index in src.get_index(model_name):
                            index_to_doctypes[index].append(model_name)
                elif options['index']:
                    index = options['index']
                    logger.info('Deleting mapping for all models on index {}.'.format(index))
                    index_to_doctypes[index] = src.get_models(index)
                else:
                    for index in src.get_indices():
                        index_to_doctypes[index] = src.get_models(index)
                    logger.info('Deleting mapping for all models ({}) on all indices ({}).'.format(index_to_doctypes.values(), index_to_doctypes.keys()))

                for index, doctype_list in iteritems(index_to_doctypes):
                    es.indices.delete_mapping(index, ','.join(doctype_list), params=None)

        elif options['action'] == 'create':
            if options['index']:
                indices = [options['index']]
            else:
                indices = src.get_indices()
            for index in indices:
                mapping = {}
                analysis = {'analyzer': {}, 'tokenizer': {}, 'filter': {}}

                for mdl_idx in src.get_model_indices(index):
                    mapping[mdl_idx.get_model().__name__] = mdl_idx.get_mapping(meta_fields=False)

                    mdl_analysis = mdl_idx.collect_analysis()
                    for key in analysis.keys():
                        value = mdl_analysis.get(key)
                        if value is not None:
                            analysis[key].update(value)

                logger.info('Creating index {} with {} doctypes.'.format(index, len(mapping)))
                es.indices.create(index=index, body={'mappings': mapping, 'settings': {'analysis': analysis}})

            es.cluster.health(index=','.join(indices), wait_for_status=wait_for_status, timeout='30s')

        elif options['action'] == 'update-mapping':
            if options['index']:
                indices = [options['index']]
            else:
                indices = src.get_indices()

            if options['models']:
                models = options['models'].split(',')
            else:
                models = []

            for index in indices:
                for model_name in src._idx_name_to_mdl_to_mdlidx[index]:
                    if models and model_name not in models:
                        continue
                    logger.info('Updating mapping of model/doctype {} on index {}.'.format(model_name, index))
                    try:
                        es.indices.put_mapping(model_name, src._idx_name_to_mdl_to_mdlidx[index][model_name].get_mapping(), index=index)
                    except Exception as e:
                        print(e)
                        if raw_input('Something terrible happened! Type "abort" to stop updating the mappings: ') == 'abort':
                            raise e
                        print('Continuing.')

        else:
            if options['index']:
                indices = options['index']
            else:
                indices = src.get_indices()
            if options['models']:
                model_names = options['models'].split(',')
            else:
                model_names = [model for index in indices for model in src.get_models(index)]

            logger.info('Updating models {} on indices {}.'.format(model_names, indices))

            # Update index.
            for model_name in model_names:
                if src.get_model_index(model_name).indexing_query is not None:
                    update_index(src.get_model_index(model_name).indexing_query, model_name, bulk_size=options['bulk_size'], num_docs=options['num_docs'], start_date=options['start_date'], end_date=options['end_date'])
                else:
                    update_index(src.get_model_index(model_name).get_model().objects.all(), model_name, bulk_size=options['bulk_size'], num_docs=options['num_docs'], start_date=options['start_date'], end_date=options['end_date'])
