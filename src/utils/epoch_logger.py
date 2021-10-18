from gensim.models.callbacks import CallbackAny2Vec
from logging import getLogger


class EpochLogger(CallbackAny2Vec):
    """Callback to log information about training"""

    def __init__(self):
        self.epoch = 0
        getLogger('gensim_node2vec').info('EpochLogger initiated, epoch = 0')

    def on_epoch_begin(self, model):
        getLogger('gensim_node2vec').info(f'Model training epoch #{self.epoch} begins')

    def on_epoch_end(self, model):
        getLogger('gensim_node2vec').info(f'Model training epoch #{self.epoch} ends')
        self.epoch += 1

    def on_train_begin(self, model):
        getLogger('gensim_node2vec').info('Model training begins')

    def on_train_end(self, model):
        getLogger('gensim_node2vec').info('Model raining ends')
