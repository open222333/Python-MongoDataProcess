from src.mongo_func import MongoSyncFunc
from src.mongo_pool import MongoConnect
from src.logger import Log

from datetime import datetime


class TestBasic():

    def __init__(self, log_name: str = 'TestBasic', **kwargs) -> None:
        """測試功能

        Args:
            log_name (str): log名稱, 預設 TestBasic
            log_level : log等級, 預設 WARNING
            size : 每次執行幾筆資料, 預設 100
        """
        self.size = kwargs.get('size', 100)  # 設置 每次執行幾筆資料
        self.logger = Log(log_name)
        self.log_level = kwargs.get('log_level', 'WARNING').upper()
        self.logger.set_level(self.log_level)
        self.logger.set_msg_handler()
        self.err_logger = Log(f'{log_name}_Error')
        self.test = False

    def enable_test(self, sleep_sec: int = 1):
        """啟用 測試模式
        """
        self.test = True
        self.logger.info('測試模式 - 不變更資料庫')
        self.sleep_sec = sleep_sec

    def enable_err_logfile(self):
        """啟用 錯誤 log 檔案紀錄
        """
        self.err_logger.set_file_handler()

    def set_size(self, size: int):
        """設置 每次執行幾筆資料

        Args:
            size (int): _description_
        """
        self.size = size


class MongoFuncBasic(TestBasic):

    def __init__(self, log_name: str = 'MongoFuncBasic', **kwargs) -> None:
        """不包含 Mongo Sync Func 類別
        基本測試功能
        檢查新舊資料是否改變 並新增或更新至mongo

            log_name (str, optional): log名稱. Defaults to 'MongoFuncBasic'.
            log_level : log等級, 預設 WARNING
            size : 每次執行幾筆資料, 預設 100
            name  (str, optional): Defaults to '未命名 Mongo 連線'
            mongo_host (str, optional): Defaults to '127.0.0.1'
            mongo_port (str, optional): Defaults to '27017'
            mongo_username (str, optional)
            mongo_password (str, optional)
            mongo_client (MongoClient) : pymongo 連線物件
        """
        super().__init__(log_name, **kwargs)
        self.mongo_client = MongoConnect(**kwargs).get_mongo_client()

    def has_change(self, old_data: dict, new_data: dict, columns: list = [], exclude_columns: list = []):
        """資料是否有更改

        Args:
            old_data (dict): _description_
            new_data (dict): _description_
            columns (list, optional): 指定比對的欄位. Defaults to [].
            exclude_columns (list, optional): 指定排除比對的欄位. Defaults to [].

        Returns:
            _type_: _description_
        """
        try:
            # self.logger.debug(f'舊資料: {old_data} 新資料: {new_data}')
            if len(columns) > 0:
                self.logger.debug(f'指定比對欄位 {columns}')
                for column_name in columns:
                    if column_name not in exclude_columns:
                        if old_data.get(column_name) or old_data.get(column_name) == False:
                            if old_data.get(column_name) != new_data.get(column_name):
                                self.logger.debug(f'{column_name} 舊資料: {old_data.get(column_name)} 新資料: {new_data.get(column_name)}')
                                return True
                            else:
                                self.logger.debug(f'{column_name} 無變化')
                        else:
                            if column_name not in old_data.keys():
                                self.logger.debug(f'舊資料 不存在{column_name}')
                                return True

            for column_name, new_value in new_data.items():
                if column_name not in exclude_columns:
                    if old_data.get(column_name) or old_data.get(column_name) == False:
                        if old_data.get(column_name) != new_value:
                            self.logger.debug(f'{column_name} 舊資料: {old_data.get(column_name)} 新資料: {new_value}')
                            return True
                        else:
                            self.logger.debug(f'{column_name} 無變化')
                    else:
                        # 沒有 column_name
                        if column_name not in old_data.keys():
                            self.logger.debug(f'舊資料 不存在{column_name}')
                            return True

            return False
        except Exception as err:
            self.logger.error(f'檢查資料是否更動 發生錯誤: {err}\n檢查欄位: {columns}\n排除欄位: {exclude_columns}\n新資料: {new_data}\n舊資料: {old_data}', exc_info=True)
            return True

    def save_to_mongo(self, database: str, collection: str, data: dict, unset: list = None, index_names: list = [], query: dict = {}, check_colunms: list = [], exclude_columns: list = ['modified_date']):
        """儲存資料至mongo

        Args:
            database (str): 指定資料庫名稱.
            collection (str): 指定集合名稱.
            data (dict): 新增或更新資料 ex: {'code': '1', 'name':'2'}
            unset (list, optional): 移除欄位 名稱. Defaults to None. ex: ['code']
            query (optional): 更新資料時需輸入查詢條件. Defaults to None. ex: comic_id=1
            index_names (list, optional): 索引欄位 若不存在則建立索引. Defaults to [].
            exclude_columns (list, optional): 指定排除比對的欄位. Defaults to ['modified_date'].
            check_colunm (list[str], optional): 更新時 只檢查指定欄位是否更改. Defaults to [].
        """
        try:
            col = self.mongo_client[database][collection]
            if data.get('_id'):
                del data['_id']

            old_data = col.find_one(query)

            if len(query) > 0 and old_data:
                update_query = {}
                if unset:
                    unset_data = {}
                    for filed in unset:
                        unset_data[filed] = 1
                    update_query['$unset'] = unset_data
                data['modified_date'] = datetime.now()
                if data.get('_id'):
                    del data['_id']
                update_query['$set'] = data
                if not self.test:
                    if isinstance(check_colunms, list) and len(check_colunms) > 0:
                        is_change = self.has_change(
                            old_data=old_data,
                            new_data=data,
                            columns=check_colunms,
                            exclude_columns=exclude_columns
                        )
                    else:
                        is_change = self.has_change(
                            old_data=old_data,
                            new_data=data,
                            exclude_columns=exclude_columns
                        )

                    if is_change:
                        self.logger.debug(f'更新資料 mongodb {database}.{collection}\n查詢條件: {query}\n內容: {update_query}\n')
                        col.update_one(query, update_query)
            else:
                data['creation_date'] = datetime.now()
                data['modified_date'] = datetime.now()
                self.logger.debug(f'新增資料 mongodb {database}.{collection}\n內容: {data}\n')
                if not self.test:
                    col.insert_one(data)

            if len(index_names) > 0:
                existing_indexes = col.index_information()
                for index_name in index_names:
                    if index_name not in existing_indexes:
                        if not self.test:
                            col.create_index(index_name)
        except Exception as err:
            self.logger.error(f'儲存資料至mongo 發生錯誤: {err}', exc_info=True)


class MongoSyncFuncBasic(MongoFuncBasic, MongoSyncFunc):

    def __init__(self, log_name: str = 'MongoSyncFuncBasic', **kwargs) -> None:
        """
            log_name (str, optional): log名稱. Defaults to 'MongoSyncFuncBasic'.
            log_level : log等級, 預設 WARNING
            size : 每次執行幾筆資料, 預設 100
            name  (str, optional): Defaults to '未命名 Mongo 連線'
            mongo_host (str, optional): Defaults to '127.0.0.1'
            mongo_port (str, optional): Defaults to '27017'
            mongo_username (str, optional)
            mongo_password (str, optional)
            mongo_client (MongoClient) : pymongo 連線物件
        """
        super().__init__(log_name, **kwargs)
