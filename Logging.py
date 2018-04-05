import logging

class Logging:

    def __init__(self,name):
        self.logger = logging.getLogger(name)
        self.handler = logging.FileHandler('hello.log')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)

    def set_log_message(self, msg, level):
        if level == 'error':
            self.logger.setLevel(logging.ERROR)
            self.handler.setLevel(logging.ERROR)
            self.logger.error(msg)

        if level == 'info':
            self.logger.setLevel(logging.INFO)
            self.handler.setLevel(logging.INFO)
            self.logger.info(msg)