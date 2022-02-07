from ctypes import *
from utils import Utils
from pathlib import Path

class BuildRS:

    def __init__(self, path=None):
        self.path = path

    
    def to_xlsx(self):
        import os
        target = 'TEST_deposits_01_2022.xls'
        target_file = os.path.join(self.path, target)
        # print(target_file)


        # SO GET MOST RECENT FILE, MAKE CHANGES BUT PRESERVE TIMING FOR EVENTUAL DISPLAY

        target_dir = os.listdir(self.path)
        for item in target_dir:
            td = os.path.join(self.path, item)
            file_stat = os.stat(td.st_ctime)
            print(file_stat)
        