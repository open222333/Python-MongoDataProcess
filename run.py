from src.mongo_sync import MongoSync

from sample.sample_func import Sample

if __name__ == "__main__":

    mongo_setting = {
        'mongo_host': '127.0.0.1',
        'mongo_port': '27017'
    }

    ms = MongoSync(**mongo_setting, log_level="DEBUG")
    sample = Sample(**mongo_setting, log_level="DEBUG")
    ms.add_func(
        func=sample.mongo_func,
        database='db',
        collection='col'
    )
    ms.run()
