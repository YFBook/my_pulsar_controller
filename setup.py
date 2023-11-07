import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

install_requires = [
    "pulsar-client==3.2.0",
    "ujson>=4.3.0",
    "python-dotenv>=0.19.0",
    "loguru>=0.5.3",
    "requests==2.31.0",
    "aiohttp==3.7.4.post0",
    "pydantic==1.8.2",
    "SQLAlchemy==1.4.9",
]
# 文件夹内需要包含__init__.py才会被该函数识别到
package = setuptools.find_packages()
print(f"package = {package}")
# 必须要调用，即使没有参数
setuptools.setup(
    name="py_sdk",  # Replace with your own username
    version="0.3.9",
    author="yif",
    author_email="xuyifeng@lingxing.com",
    description="常用的puthon第三方库二次封装使用",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: POSAIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ],
    install_requires=install_requires,
    packages=package,
    python_requires=">=3.8",
)
