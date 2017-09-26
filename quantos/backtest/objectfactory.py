# encoding: UTF-8


########################################################################
class ObjectCreator():
    @staticmethod
    def create(name):
        dict = globals()
        if dict.has_key(name):
            return dict[name]()
        
        return None
