from src.logger import Log

class TestBasic():

    def __init__(self, log_name: str = 'TestBasic', **kwargs) -> None:
        """測試功能

        Args:
            log_name (str): _description_
            log_level : log等級
            size : 每次執行幾筆資料
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

