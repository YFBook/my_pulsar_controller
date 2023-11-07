# -*- coding: utf-8 -*-
import hashlib, os, sys
import base64
import hmac
import secrets
from Crypto.Cipher import AES


def get_key(key):
    hash = hashlib.sha1(key.encode("utf-8"))  # 哈希密匙
    key = str(hash.hexdigest()[8:16]) + key  # 真实密匙
    while len(key) % 32 != 0:
        key += "\0"
    return bytes(key, encoding="utf8")


def encrypt(string, key):
    iv = secrets.token_bytes(AES.block_size)

    string = string.encode("utf-8")
    while len(string) % 32 != 0:
        string += b"\x07"
    cipher = AES.new(get_key(key), AES.MODE_CBC, iv)
    ciphertextRaw = cipher.encrypt(string)  # 加密数据
    hash_hmac = hmac.new(get_key(key), ciphertextRaw, digestmod=hashlib.sha256).digest()
    return str(base64.b64encode(iv + hash_hmac + ciphertextRaw), "utf-8")


def decrypt(string, key):
    plantext = base64.b64decode(string)  # 使用base64解密
    iv, hmac_text, ciphertextRaw = (
        plantext[:16],
        plantext[16 : 16 + 32],
        plantext[16 + 32 :],
    )  # 获取iv,加密文本
    cipher = AES.new(get_key(key), AES.MODE_CBC, iv)

    text = cipher.decrypt(ciphertextRaw)
    hash_hmac = hmac.new(get_key(key), ciphertextRaw, digestmod=hashlib.sha256).digest()
    if hash_hmac == hmac_text:
        text = "".join([x if ord(x) > 15 else "" for x in text.decode("utf-8")])
        return text
    else:
        return False
