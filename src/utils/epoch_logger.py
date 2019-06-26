from gensim.models.callbacks import CallbackAny2Vec
import logging.config

logging.config.fileConfig('src/logging.conf')

class EpochLogger(CallbackAny2Vec):
    """Callback to log information about training'"""

    def __init__(self):
        self.logger = logging.getLogger('train_node2_vec_model.epoch_logger')
        self.epoch = 0

    def on_epoch_begin(self, model):
        self.logger.info(f'Model training epoch #{self.epoch} begun')

    def on_epoch_end(self, model):
        self.logger.info(f'Model training epoch #{self.epoch} ended')
        self.epoch += 1
