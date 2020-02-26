from azure.storage.filedatalake import DataLakeServiceClient

class ServiceClientFactory:

    def __init__(
        self,
        storageAccountName: str,
        storageAccountKey: str
    ):
        self.__storageAccountName = storageAccountName
        self.__storageAccountKey = storageAccountKey

    def create(self):
        return DataLakeServiceClient(
            account_url='{}://{}.dfs.core.windows.net'.format('https', self.__storageAccountName),
            credential=self.__storageAccountKey
        )
