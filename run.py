from src.mongo_sync import MongoSync
from src.basic import MongoFuncBasic


class Sample(MongoFuncBasic):

    def __init__(self, database: str, collection: str, log_name: str = 'Sample', **kwargs) -> None:
        super().__init__(database, collection, log_name, **kwargs)

    def mongo_func(self, data, **kwargs):
        '''

        custom code


        '''


if __name__ == "__main__":

    mongo_setting = {
        'mongo_host': '127.0.0.1',
        'mongo_port': '27017'
    }

    general_setting = {
        'log_level': "DEBUG"
    }

    settings = [
        {
            'database': 'db1',
            'collection': 'col1',
            'args1': 'a'
        },
        {
            'database': 'db2',
            'collection': 'col2',
            'args1': 'b'
        }
    ]

    mongo_setting.update(general_setting)
    ms = MongoSync(**mongo_setting)

    for setting in settings:

        setting.update(general_setting)

        ms.add_func(
            func=Sample(
                **mongo_setting,
                **setting
            ).mongo_func,
            **setting
        )
    ms.run()
