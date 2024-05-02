from src.mongo_pattern import MongoUriPattern
from src.mongo_pool import MongoConnectPool
from src.basic import TestBasic


from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
from time import sleep


class MongoSync(TestBasic):

    def __init__(self, log_name: str = 'MongoSync', **kwargs) -> None:
        """同步

        Args:
            size (int): 每次處理幾筆
            log_level (str, optional): log等級. Defaults to WARNING.
            name  (str, optional): Defaults to '未命名 Mongo 連線'
            mongo_host (str, optional): Defaults to '127.0.0.1'
            mongo_port (str, optional): Defaults to '27017'
            mongo_username (str, optional)
            mongo_password (str, optional)

        Returns:
            _type_: _description_
        """
        super().__init__(log_name, **kwargs)

        # mongo 連線設定
        self.mongo_host = kwargs.get('mongo_host', '127.0.0.1')
        self.mongo_port = kwargs.get('mongo_port', '27017')
        self.mongo_username = kwargs.get('mongo_username')
        self.mongo_password = kwargs.get('mongo_password')
        self.generate_mongo_uri()

        self.mongo_pool = MongoConnectPool()
        self.mongo_pool.set_mongo_connect_uri(self.uri)

        self.default_setting = {
            'mongo_host': self.mongo_host,
            'mongo_port': self.mongo_port,
            'mongo_username': self.mongo_username,
            'mongo_password': self.mongo_password
        }

        self.funcs = {}

    def generate_mongo_uri(self, **kwargs):
        """生成 mongo uri
        """
        host = kwargs.get('host', self.mongo_host)
        port = kwargs.get('port', self.mongo_port)
        username = kwargs.get('username', self.mongo_username)
        password = kwargs.get('password', self.mongo_password)

        mup = MongoUriPattern()
        uri_pattern = mup.uri_pattern
        ip_pattern = mup.ip_pattern
        host_pattern = mup.host_pattern
        if bool(uri_pattern.match(host)):
            uri = host
        elif bool(ip_pattern.match(host)):
            uri = host
        elif bool(host_pattern.match(host)):
            uri = host
        else:
            if port:
                uri = f'{host}:{port}'
            else:
                uri = f'{host}'

            if username and password:
                uri = f'{username}:{password}@{uri}'

            uri = f'mongodb://{uri}/'

        if not bool(uri_pattern.match(uri)):
            raise ValueError(f'生成 mongo uri 發生錯誤: {uri}')
        else:
            self.logger.info(f'生成 mongo uri : {uri}')
        self.uri = uri

    def get_mongo_total_amount(self, mongo_client: MongoClient, collection: str, database: str, query: dict = {}):
        """取得 mongo 資料總數量

        Args:
            database (str): 資料庫
            collection (str): 集合
            query (dict, optional): 查詢條件. Defaults to {}.

        Returns:
            _type_: _description_
        """
        from pymongo import version as pymongo_version
        from pymongo.collection import Collection

        try:
            col = mongo_client[database][collection]
            if isinstance(col, Collection):
                major_version, minor_version, _ = map(
                    int, pymongo_version.split('.'))
                if major_version == 3 and minor_version < 11:
                    count = col.find(query).count()
                else:
                    count = col.count_documents(query)
                return count
            else:
                raise TypeError(f'參數帶入錯誤型態 col type: {type(col)}')
        except Exception as err:
            self.logger.error(
                f'取得 mongo {database} {collection} 資料總數量 發生錯誤: {err}', exc_info=True)
            return None

    def process_mongo_datas(self, func, database: str, collection: str, **kwargs):
        """處理 mongo 資料

        Args:
            collection (str): 集合
            func (_type_): 執行的函式
            database (str): 資料庫.
            limit (int, optional): 執行幾筆
            mongo_client : 若處理函式的 mongo 主機與批量資料處理的不同, 需帶入 mongo_client
        """
        try:
            query = kwargs.get('query', {})
            limit = int(kwargs.get('limit', 0))
            connect_uuid = self.mongo_pool.get_connect()
            mongo_client = self.mongo_pool.pool[connect_uuid]

            # 若處理函式的 mongo 主機不同, 需帶入 mongo_client
            func_mongo_client = kwargs.get('mongo_client', mongo_client)

            if not isinstance(mongo_client, MongoClient):
                raise TypeError('mongo_client 設定錯誤')
            col = mongo_client[database][collection]

            if limit:
                self.logger.debug(f'限制執行 {limit} 筆')

            total = self.get_mongo_total_amount(
                mongo_client=mongo_client, database=database, collection=collection, query=query)
            self.logger.info(f'總量: {total}')
            start = int(kwargs.get('start', 0))
            per = self.size
            count = 0
            stop_process = False
            while True:

                end = start + per
                if end >= total:
                    end = total

                datas = col.find(query)[start:end]
                for data in datas:
                    count += 1
                    self.logger.debug(f'{count}/{total}')
                    func(data=data, mongo_client=func_mongo_client)

                    # 測試模式 每筆停止一秒
                    if self.test:
                        sleep(self.sleep_sec)

                    if limit != None:
                        if count == limit:
                            self.logger.debug(f'已執行 {count} 筆, 中止程式')
                            stop_process = True
                            break

                if end == total or stop_process:
                    break

                start += per

            self.mongo_pool.del_connect(connect_uuid)
        except Exception as err:
            self.logger.error(f'處理 mongo 資料 發生錯誤: {err}', exc_info=True)

    def add_func(self, func, database: str, collection: str, limit: int = 0,  query: dict = {}, **kwargs):
        """新增 要執行的函式

        Args:
            database (str): 資料庫.
            collection (str): 取得資料的需使用的 collection 名稱
            limit (int, optional): 測試回數. Defaults to 0.
            query (dict, optional): 查詢條件. Defaults to {}.
            mongo_client : MongoConnect 連線物件
        """
        self.funcs[func] = {
            'database': database,
            'collection': collection,
            'limit': limit,
            'query': query,
            **kwargs
        }
        self.logger.info('新增函式')
        self.logger.debug(f'新增函式: {func} {self.funcs[func]}')

    def run(self, workers: int = 3):
        """執行

        Args:
            workers (int, optional): 多執行序數量. Defaults to 3.
        """
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for func, details in self.funcs.items():
                self.logger.info('執行函式')
                self.logger.debug(f'執行函式: {func} {details}')
                if isinstance(details, dict):
                    executor.submit(
                        self.process_mongo_datas,
                        func=func,
                        **details
                    )
