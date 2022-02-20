from config import Config
from file_indexer import FileIndexer

class BuildRs:
    def __init__(self, mode=None):
        if mode == 'testing':
            self.mode = 'testing'
            self.findex = FileIndexer(path=Config.TEST_RS_PATH, discard_pile=Config.TEST_MOVE_PATH, db=Config.test_findex_db, table='findex')


    def index_wrapper(self):
        print(self.findex)

    def build_rs_runner(self):
        self.index_wrapper()


if __name__ == '__main__':
    buildrs = BuildRs(mode='testing')
    buildrs.index_wrapper()
