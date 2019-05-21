import logging.config
import multiprocessing
import runpy


def get_content_store_data():
    runpy.run_module('src.data_preprocessing.get_content_store_data', run_name='__main__')


def make_functional_edges_and_weights():
    runpy.run_module('src.data_preprocessing.make_functional_edges_and_weights', run_name='__main__')


if __name__ == '__main__':
    logging.config.fileConfig('src/logging.conf')
    module_logger = logging.getLogger('run_all')

    functional_edges_and_weights = multiprocessing.Process(
        name='make_functional_edges_and_weights', target=make_functional_edges_and_weights)
    content_store_data = multiprocessing.Process(name='get_content_store_data', target=get_content_store_data)

    functional_edges_and_weights.start()
    module_logger.info('kicked off make_functional_edges_and_weights')

    content_store_data.start()
    module_logger.info('kicked off get_content_store_data')

    functional_edges_and_weights.join()
    module_logger.info('make_functional_edges_and_weights is finished')
    content_store_data.join()
    module_logger.info('get_content_store_data is finished')

    module_logger.info('running make_network')
    runpy.run_module('src.features.make_network', run_name='__main__')

    module_logger.info('running train_node2vec_model')
    runpy.run_module('src.models.train_node2vec_model', run_name='__main__')
