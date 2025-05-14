import abc
import logging
from typing import Generic, Optional, TypeVar

from dbami.db import DB

T = TypeVar("T")


class Helper(abc.ABC, Generic[T]):
    def __init__(
        self,
        dbami_db: DB,
        logger: logging.Logger,
        helper_config: Optional[T] = None,
        **connect_kwargs,
    ) -> None:
        self.database = dbami_db
        self.logger = logger
        self.connect_kwargs = connect_kwargs

        if helper_config is None:
            helper_config = self.get_config_class()

        self.config = helper_config

    @classmethod
    @abc.abstractmethod
    def get_config_class(cls) -> T:
        pass

    @abc.abstractmethod
    async def run(self) -> None:
        pass
