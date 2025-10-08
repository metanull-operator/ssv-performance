from .storage_clickhouse import ClickHouseStorage
from typing import Dict, Any


##
## Factory class to manage storage instances
##
class StorageFactory:
    _instances: Dict[str, Any] = {}

    @staticmethod
    def initialize(storage_name:str , storage_type: str, **kwargs) -> None:
        if storage_name not in StorageFactory._instances:
            if storage_type == "ClickHouse":
                StorageFactory._instances[storage_name] = ClickHouseStorage(**kwargs)
        else:
            raise Exception(f"{storage_name} storage is already initialized")

    @staticmethod
    def get_storage(storage_name: str) -> Any:
        if storage_name not in StorageFactory._instances:
            raise Exception(f"{storage_name} storage has not been initialized")
        return StorageFactory._instances[storage_name]