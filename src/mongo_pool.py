from src.logger import Log
from src.mongo_pattern import MongoUriPattern

from pymongo import MongoClient
import random
import uuid


class MongoConnect():

    def __init__(self, **kwargs) -> None:
        """建立 Mongo 連線

        Args:
            name  (str, optional): Defaults to '未命名 Mongo 連線'
            mongo_host (str, optional): Defaults to '127.0.0.1'
            mongo_port (str, optional): Defaults to '27017'
            mongo_username (str, optional)
            mongo_password (str, optional)
            log_level (str, optional): Defaults to ''

        Returns:
            _type_: _description_
        """
        self.name = kwargs.get('name', '未命名 Mongo 連線')
        self.logger = Log(self.name)
        self.logger.set_level(str(kwargs.get('log_level', 'WARNING')).upper())
        self.logger.set_msg_handler()

        # mongo 連線設定
        self.mongo_host = kwargs.get('mongo_host', '127.0.0.1')
        self.mongo_port = kwargs.get('mongo_port', '27017')
        self.mongo_username = kwargs.get('mongo_username')
        self.mongo_password = kwargs.get('mongo_password')

    def __call__(self):
        """_summary_

        Returns:
            Any: _description_
        """
        return self.get_mongo_client()

    def get_mongo_client(self, **kwargs) -> MongoClient:
        """取得 mongo 連線

        Returns:
            _type_: _description_
        """
        try:
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

                uri = f'mongodb://{uri}'

            self.mongo_setting = {
                'mongo_host': host,
                'mongo_port': port,
                'mongo_username': username,
                'mongo_password': password,
                'uri': uri
            }

            self.logger.debug(f'mongo_client 設定:\n{self.mongo_setting}')
            mongo_client = MongoClient(uri)
            return mongo_client
        except Exception as err:
            self.logger.error(f'取得 mongo 連線 發生錯誤: {err}', exc_info=True)

    def generate_mongo_uri(self, **kwargs):
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

            uri = f'mongodb://{uri}'

    def get_mongo_client_setting(self):
        return self.mongo_setting


class MongoConnectPool():

    pool = {}
    in_use = []
    idle = []

    def __init__(self, max_connect: int = 100, **kwargs) -> None:
        """
            connect_name: 名稱,預設 Mongo 連線
            log_level: log 等級
            logger: Log物件
            max_connect: 最多連線數, 預設 100
        """
        self.connect_name = kwargs.get('connect_name', 'Mongo 連線')
        self.logger = Log('MongoConnectPool')
        self.logger.set_level(str(kwargs.get('log_level', 'WARNING')).upper())
        self.logger.set_msg_handler()
        self.max_connect = max_connect

    def set_mongo_connect_uri(self, uri: str):
        """設定 mongo 連線

        Args:
            uri (str): _description_
        """
        # mongo 連線設定
        self.uri = uri

        uri_pattern = MongoUriPattern().uri_pattern
        if bool(uri_pattern.match(uri)):
            self.uri = uri
        else:
            raise SyntaxError(f'uri 格式錯誤, 參考 https://www.mongodb.com/docs/manual/reference/connection-string/')

    def switch_status(self, connect_uuid):
        """切換狀態

        Args:
            connect_uuid (_type_): _description_
        """
        if connect_uuid in self.in_use:
            self.logger.info(f'切換狀態 {connect_uuid}: 閒置')
            self.in_use.remove(connect_uuid)
            self.idle.append(connect_uuid)

        if connect_uuid in self.idle:
            self.logger.info(f'切換狀態 {connect_uuid}: 使用中')
            self.idle.remove(connect_uuid)
            self.in_use.append(connect_uuid)

    def generate_uuid(self):
        """生成 uuid

        Returns:
            _type_: _description_
        """
        try:
            if len(self.pool) < self.max_connect:
                connect_uuid = uuid.uuid1()
                if connect_uuid not in self.pool.keys():
                    return connect_uuid
            else:
                return None
        except Exception as err:
            self.logger.error(f'生成 uuid 發生錯誤: {err}')

    def add_connect(self):
        """新增連線

        Returns:
            _type_: _description_
        """
        try:
            connect_uuid = self.generate_uuid()
            if connect_uuid != None:
                self.pool[connect_uuid] = MongoConnect(
                    mongo_host=self.uri,
                    name=f'{self.connect_name} {connect_uuid}'
                ).get_mongo_client()
                self.idle.append(connect_uuid)
                self.logger.info(f'新增連線 {connect_uuid}')
                return connect_uuid
            else:
                return None
        except Exception as err:
            self.logger.error(f'新增連線 發生錯誤: {err}')

    def get_connect_pool(self):
        """取得 連線池

        Returns:
            _type_: _description_
        """
        return self.pool

    def get_idle_connect(self):
        """取得 閒置連線

        Returns:
            _type_: _description_
        """
        return self.idle

    def get_in_use_connect(self):
        """取得 使用中連線

        Returns:
            _type_: _description_
        """
        return self.in_use

    def del_connect(self, connect_uuid: str):
        """刪除連線

        Args:
            connect_uuid (str): _description_
        """
        try:
            self.logger.info(f"執行刪除連線: {connect_uuid}")

            if connect_uuid in self.idle:
                self.idle.remove(connect_uuid)
                self.logger.debug("檢查閒置連線")

            if connect_uuid in self.in_use:
                self.in_use.remove(connect_uuid)
                self.logger.debug("檢查使用中連線")

            if self.pool.get(connect_uuid):
                self.pool[connect_uuid].close()
                del self.pool[connect_uuid]
                self.logger.info(f"{connect_uuid} 已刪除")
            else:
                self.logger.info(f"{connect_uuid} 不存在")
        except Exception as err:
            self.logger.error(f'刪除連線 發生錯誤: {err}')

    def get_connect(self):
        """取得連線
        """
        try:
            if len(self.idle) == 0:
                self.add_connect()
            self.logger.info(f"執行取得連線")
            connect_uuid = random.choice(self.idle)
            self.switch_status(connect_uuid)
            return connect_uuid
        except Exception as err:
            self.logger.error(f'取得連線 發生錯誤: {err}')

    def release_connect(self, connect_uuid):
        """釋放連線
        """
        try:
            self.logger.info(f"執行釋放連線 {connect_uuid}")
            if connect_uuid in self.in_use:
                self.switch_status(connect_uuid)
            else:
                raise RuntimeError(f'{connect_uuid} 非使用中連線')
        except Exception as err:
            self.logger.error(f'釋放連線 發生錯誤: {err}')
