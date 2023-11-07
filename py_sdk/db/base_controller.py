import os
import sys

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
)
import asyncio
from typing import List
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.exc import TimeoutError

from functools import wraps
from py_sdk.util.logger import logger


import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from sqlalchemy.orm import sessionmaker
from py_sdk.service.config_center_service import ConfigCenterService

db_monitor_url = "https://oapi.dingtalk.com/robot/send?access_token=7d0afe6e9f12d1639bbf62a90e2988b067e849090784bda7a14151b2a7355ca0"


class BaseController:
    __dsn_map = {}

    @classmethod
    async def New(
        cls,
        db_name,
        config_center_host,
        app_id,
        key,
        buss_code,
    ):
        service = ConfigCenterService(
            host=config_center_host, app_id=app_id, key=key, buss_code=buss_code
        )

        db_info = await service.get_db_with_db_name(db_name=db_name)

        db = cls(db_info=db_info)
        await service.close()
        return db

    def __init__(
        self,
        db_info={},
        pool_recycle=60 * 5,
        connect_timeout=60,
        max_execution_time=1000 * 60 * 5,
        wait_timeout=60 * 10,
        innodb_lock_wait_timeout=60 * 2,
        pool_size=100,
        pool_max_overflow=100,
        pool_timeout=60,
        controller_exec_timeout=600,
    ):
        """
        pool_recycle: 设置连接多久后会被回收,失效时间只会发生在获取新的connection；-1表示不回收，但是mysql默认在八小时内未在连接上检测到任何活动会自动断开连接；只能小于或等于wait_timeout，否则当连接被关闭后会出现Lost connection to MySQL server during query的错误
        connect_timeout: session连接数据库超时时间，单位s
        max_execution_time: 单位ms;该参数控制了单条查询的最大执行时间，以秒为单位。如果查询超过了该时间限制，MySQL会自动终止该查询并返回错误。通过将该参数设置为一个较小的值，可以防止一些耗时较长的查询对系统的影响。
        wait_timeout: 单位s;该参数控制了MySQL连接的空闲时间。如果连接在该时间内没有任何操作，则会被MySQL服务器断开。通过将该参数设置为一个较小的值，可以释放不活跃的连接，从而减少系统负担和资源占用。
        innodb_lock_wait_timeout: 单位s;该参数控制了InnoDB引擎获取锁的等待时间。如果一个事务不能在该时间内获取到需要的锁，则会被MySQL自动回滚。通过将该参数设置为一个较小的值，可以避免因锁等待导致的系统资源浪费。
        pool_size: db连接池数
        max_overflow: 连接数达到pool_size后允许再新建的连接数，故最大连接数 = pool_size + max_overflow
        pool_timeout:获取连接的时间(单位s),默认60秒; session只有在执行excute时才会去获取连接
        db操作超时时间: (单位:s); 防止max_execution_time不生效的兜底机制, 故应该大于max_execution_time
        """
        self.reset_db_info(db_info)
        self.pool_recycle = pool_recycle
        self.connect_timeout = connect_timeout
        self.max_execution_time = max_execution_time
        self.wait_timeout = wait_timeout
        self.innodb_lock_wait_timeout = innodb_lock_wait_timeout
        self.pool_size = pool_size
        self.pool_max_overflow = pool_max_overflow
        self.pool_timeout = pool_timeout
        self.controller_exec_timeout = controller_exec_timeout
        if controller_exec_timeout * 1000 < max_execution_time:
            raise Exception("max_execution_time必须小于controller_exec_timeout")

    def reset_db_info(self, db_info):
        self.__db_info = db_info

    def set_db_setting(
        self,
        pool_recycle=60 * 5,
        connect_timeout=60,
        max_execution_time=1000 * 60 * 5,
        wait_timeout=60 * 10,
        innodb_lock_wait_timeout=60 * 2,
        pool_size=100,
        pool_max_overflow=100,
        pool_timeout=60,
        controller_exec_timeout=600,
    ):
        self.pool_recycle = pool_recycle
        self.connect_timeout = connect_timeout
        self.max_execution_time = max_execution_time
        self.wait_timeout = wait_timeout
        self.innodb_lock_wait_timeout = innodb_lock_wait_timeout
        self.pool_size = pool_size
        self.pool_max_overflow = pool_max_overflow
        self.pool_timeout = pool_timeout
        self.controller_exec_timeout = controller_exec_timeout
        if controller_exec_timeout * 1000 < max_execution_time:
            raise Exception(
                "set_db_setting异常,max_execution_time必须小于controller_exec_timeout"
            )

    @staticmethod
    def get_db_dsn(db_info) -> str:
        dsn = "mysql+aiomysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4".format(
            **db_info
        )
        return dsn

    def generate_async_session_maker(
        self,
        is_sr=False,
        **kwargs,
    ) -> sessionmaker:
        """
        kwargs:
            poolclass=None, NullPool表示不适用连接池
            pool_recycle=-1， 回收连接时间， -1表示不回收，但是mysql默认在八小时内未在连接上检测到任何活动会自动断开连接
        """
        if is_sr:
            init_command = f"""set max_execution_time={self.max_execution_time};set wait_timeout={self.wait_timeout};"""
        else:
            init_command = f"""set max_execution_time={self.max_execution_time};set wait_timeout={self.wait_timeout};set innodb_lock_wait_timeout={self.innodb_lock_wait_timeout};"""
        dsn = BaseController.get_db_dsn(db_info=self.__db_info)
        # 这里并发情况可能出现多个线程创建同样dsn的engine，后面考虑要不要加锁
        if dsn in BaseController.__dsn_map:
            session_maker = BaseController.__dsn_map[dsn][1]
        else:
            engine = create_async_engine(
                dsn,
                pool_recycle=self.pool_recycle,
                pool_size=self.pool_size,
                max_overflow=self.pool_max_overflow,
                pool_timeout=self.pool_timeout,
                connect_args={
                    "connect_timeout": self.connect_timeout,
                    "init_command": init_command,
                },
                **kwargs,
            )
            session_maker = sessionmaker(
                bind=engine,
                class_=AsyncSession,
                autoflush=False,
                autocommit=False,
                expire_on_commit=False,
            )
            session_maker.dsn = dsn
            BaseController.__dsn_map[dsn] = (engine, session_maker)
        return session_maker

    def __init_session(fn):
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            __session_maker = self.generate_async_session_maker()
            async with __session_maker() as async_session:
                kwargs["async_session"] = async_session
                return await fn(self, *args, **kwargs)

        return wrapper

    async def close(self):
        if self.__db_info:
            dsn = self.get_db_dsn(db_info=self.__db_info)
            if dsn not in BaseController.__dsn_map:
                return
            await BaseController.__dsn_map[dsn][0].dispose()
            del BaseController.__dsn_map[dsn]

    @property
    def db_info(self):
        return self.__db_info

    def get_dsn(self):
        if not self.__db_info:
            raise Exception("没有db info，需要先执行open函数")
        return BaseController.get_dns(db_info=self.__db_info)

    def clean_df(self, model, df_value: pd.DataFrame):
        miss_cols = ["gmt_create", "gmt_modified", "id"]
        select_cols = [
            c.name
            for c in model.__table__.c
            if c.name not in miss_cols and c.name in df_value.columns
        ]
        return df_value[select_cols].to_dict("records")

    def get_select_cols(self, model, miss_cols=["gmt_create", "gmt_modified"]):
        return [c for c in model.__table__.c if c.name not in miss_cols]

    @classmethod
    async def timeout(cls, session: AsyncSession, timeout):
        """
        超过预设超时时间后把对应session的连接释放掉
        """
        try:
            await asyncio.sleep(timeout)
            # 经测试，rollback操作可以释放掉连接，会释放掉占用的连接数，但可能会抛出异常
            await session.rollback()
            logger.info("db execute timeout - session rollback success")
        except Exception as e:
            logger.warning(f"db execute timeout - err = {e}")
        finally:
            await session.close()

    @__init_session
    async def execute_mul_stmt(
        self,
        list_stmt,
        async_session: AsyncSession,
    ):
        """
        多个操作在同一个事务中执行
        """
        # 生成超时任务，防止db操作一直堵塞着协程
        task_execute_timeout = asyncio.create_task(
            BaseController.timeout(
                async_session,
                timeout=self.controller_exec_timeout * len(list_stmt),
            )
        )

        error = None
        try:
            list_result_count = []
            for stmt in list_stmt:
                result = await async_session.execute(stmt)
                list_result_count.append(result.rowcount)
            await async_session.commit()
            return list_result_count
        except TimeoutError as e:
            error = e
            # TimeoutError: 从连接池获取连接超时
            await async_session.rollback()
        except Exception as e:
            error = e
            await async_session.rollback()
        finally:
            # db操作结束后就把超时任务给取消掉
            task_execute_timeout.cancel()
            if error:
                raise error

    @__init_session
    async def execute(self, stmt, async_session: AsyncSession, values=""):
        # 生成超时任务，防止db操作一直堵塞着协程
        task_execute_timeout = asyncio.create_task(
            BaseController.timeout(async_session, timeout=self.controller_exec_timeout)
        )

        error = None
        try:
            if values:
                result = await async_session.execute(stmt, values)
            else:
                result = await async_session.execute(stmt)
            await async_session.commit()
            return result
        except TimeoutError as e:
            error = e
            # TimeoutError: 从连接池获取连接超时
            await async_session.rollback()
        except Exception as e:
            error = e
            await async_session.rollback()
        finally:
            # db操作结束后就把超时任务给取消掉
            task_execute_timeout.cancel()
            if error:
                raise error

    async def select(self, stmt) -> List:
        result = await self.execute(stmt=stmt)
        return [dict(row) for row in result]

    async def update(self, stmt, update_value=""):
        result = await self.execute(stmt=stmt, values=update_value)
        return result.rowcount

    async def insert(self, stmt, insert_values=""):
        result = await self.execute(stmt=stmt, values=insert_values)
        return result.rowcount

    async def delete(self, stmt):
        result = await self.execute(stmt=stmt)
        return result.rowcount


"""
db操作超时问题
在还在操作db的时候把协程cancel掉，连接会继续被占用， 后续db操作使用同一个连接池时，因为连接数已经满了，所有execute都会被阻塞住。
1. 协程被cancel后，对engine执行dispose函数，可以释放前面被占用的连接,但同时之前的连接所有的操作在收到db响应后都会发生异常（sqlalchemy.exc.OperationalError）。
2. 协程被cancel后，已发生db操作（执行execute）的连接都会异常(pymysql.err.InterfaceError)
3. 获取连接超时会抛出 sqlalchemy.exc.TimeoutError异常（session在执行execute的时候才会去获取连接）
4. 每次execute执行完毕，连接都会重新返回连接池,等待下一次execute操作.
5. 连接的配置在返回连接池后依旧生效，如:session执行set max_execution_time=20000后，下一个获取到该连接依旧生效
6. max_execution_time生效后，执行超时会异常(pymysql.err.InternalError)
"""
