项目中常见第三方数据库（如aiokafka、pulsar-client等），进行二次分装后的sdk，方便业务直接使用

# 先升级打包工具
pip install --upgrade setuptools wheel twine

* 删除之前的打包文件
sh remove_build.sh

# 打包(确定已经更改了版本号)
python setup.py sdist bdist_wheel

# 检查
twine check dist/*

# 上传pypi
twine upload dist/*  

# 安装最新的版本测试
pip install -U py_sdk 

# 安装指定版本
pip3 install py_sdk  

# 其他注意
1. 如果需要重新打包，需要把上一次打包生成的文件夹删除
2. 重新上传的包，版本号不能和已经上传的包的版本号一样。

	
 