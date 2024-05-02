from abc import ABC, abstractmethod


class MongoSyncFunc(ABC):

    """抽象類別

    使用 MongoSync 類別
    加入 funcs 的規範
    """

    @abstractmethod
    def mongo_func(self, data, **kwargs):
        pass
