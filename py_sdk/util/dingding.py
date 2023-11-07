import json

import requests
from py_sdk.util.logger import logger


class DingDingService(object):
    @staticmethod
    def send_text_by_robot(url, content, at_mobiles=None, is_at_all=False):
        """
        通过群机器人发送text消息
        """
        if url == "":
            logger.info("没有配置告警群，无需推送")
            return
        try:
            headers = {"Content-Type": "application/json ;charset=utf-8 "}
            data = {
                "msgtype": "text",
                "text": {"content": content},
                "at": {"atMobiles": at_mobiles, "isAtAll": is_at_all},
            }
            requests.post(url, json=data, headers=headers)
        except Exception as error:
            logger.exception(f"发送告警信息失败: {error.__str__()}")

    @staticmethod
    def send_markdown_by_robot(url, title, text, at_mobiles=None, is_at_all=False):
        """
        通过群机器人发送markdown类型消息
        """
        headers = {"Content-Type": "application/json ;charset=utf-8 "}
        data = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text},
            "at": {"atMobiles": at_mobiles, "isAtAll": is_at_all},
        }
        data = json.dumps(data)
        ret = requests.post(url, data, headers=headers)
