# pylint: disable = invalid-name, too-many-locals, too-many-branches, too-many-statements, too-many-nested-blocks
from logging import Logger
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from box import Box
from datalakebundle.acl.set.AclStringGenerator import AclStringGenerator

class AclExporter:

    def __init__(
        self,
        configDir: str,
        storageName: str,
        logger: Logger,
        activeDirectory: Box,
        serviceClient: DataLakeServiceClient,
        aclStringGenerator: AclStringGenerator,
    ):
        self.__configDir = configDir
        self.__storageName = storageName
        self.__logger = logger
        self.__activeDirectory = activeDirectory
        self.__serviceClient = serviceClient
        self.__aclStringGenerator = aclStringGenerator

    def export(self, storage_filesystem, maxLevel, export_path):
        info_dict = {
            'storage_account': self.__storageName,
            'storage_filesystem': storage_filesystem,
            'maxLevel': maxLevel,
            'export_path': export_path
        }
        self.__logger.info('Acl Export started.', extra=info_dict)

        yaml = ''
        self.__logger.debug('------------  Active directory section -----------------------------')
        yaml = yaml + self.__add_activeDirectory_users_from_config()
        self.__logger.debug(f'Active Directory Table.', extra=self.__activeDirectory)

        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)
        paths = file_system_client.get_paths()
        self.__logger.debug('get_paths() initialized', extra={'path': paths})

        self.__logger.debug('------------  Storage section -----------------------------')
        yaml = yaml + self.__generate_yaml_key_value('storage_account', self.__storageName, 0)

        self.__logger.debug('------------  Filesystem section -----------------------------')
        directory_client = file_system_client.get_directory_client('.')
        self.__logger.debug(f'directory_client: {directory_client}')
        acl_props = directory_client.get_access_control()
        self.__logger.debug(f'acl_props: {acl_props}')
        len_path_split = 0
        yaml = yaml + self.__generate_yaml_key_only(storage_filesystem, len_path_split)

        yaml = yaml + self.__generate_yaml_key_only('acl', len_path_split + 1)
        df = self.__aclStringGenerator.create_folder_properties_pd_table(acl_props['acl'])

        for acl_type_item in self.__get_available_acl_types(df):

            yaml = yaml + self.__generate_yaml_key_only(f'{acl_type_item}s', len_path_split + 2)
            for acl_aad_oid_item in self.__get_aad_oid_list_per_type(df, acl_type_item):

                yaml = yaml + self.__generate_yaml_key_only(f'- {acl_type_item}', len_path_split + 3)

                df_filtered = self.__filter_relevant_aad_oid_and_acl_type(df, acl_aad_oid_item, acl_type_item)

                group_set = False
                for index, row in df_filtered.iterrows():
                    self.__logger.debug(f"index: {index}")
                    if not group_set:
                        yaml = yaml + self.__generate_yaml_key_value('name', self.__get_name_for_aad_objectid
                                                (row['acl_aad_object_id'], self.__activeDirectory), len_path_split + 5)
                        group_set = True

                    if row['acl_level'] == 'access':
                        yaml = yaml + self.__generate_yaml_key_value('acl_access', row['acl_rights'], len_path_split + 5)
                    elif row['acl_level'] == 'default':
                        yaml = yaml + self.__generate_yaml_key_value('acl_default', row['acl_rights'], len_path_split + 5)
                    else:
                        self.__logger.warning(f"WARN: WRONG ACL VALUE: {row['acl_level']}")
        yaml = yaml + self.__generate_yaml_key_only('folders', 1)

        self.__logger.debug('------------  Storage Folders section -----------------------------')
        # Section specific params
        level_indent = 1
        max_level = 1
        previous_level = 0

        for path in paths:
            self.__logger.debug('------------------')
            if len(path.name.split('/')) <= maxLevel:
                _full_path, _current_folder, len_path_split, acl_props, is_folder = self.__get_folder_properties_from_sdk(path, storage_filesystem)

                if is_folder:
                    if len_path_split <= maxLevel:  # variable defines how deep we want to go
                        # Initializing level indent
                        setFolder = False
                        if max_level < len_path_split:
                            level_indent += 1
                            max_level = len_path_split

                        elif max_level > len_path_split:
                            level_indent = len_path_split
                            max_level = len_path_split
                            previous_level = len_path_split - 1
                        else:
                            setFolder = True

                        yaml = yaml + self.__generate_yaml_key_only(_current_folder, len_path_split + level_indent)
                        yaml = yaml + self.__generate_yaml_key_only('acl', len_path_split + 1 + level_indent)

                        df = self.__aclStringGenerator.create_folder_properties_pd_table(acl_props['acl'])
                        for acl_type_item in self.__get_available_acl_types(df):
                            yaml = yaml + self.__generate_yaml_key_only(f'{acl_type_item}s', len_path_split + 2 + level_indent)
                            for acl_aad_oid_item in self.__get_aad_oid_list_per_type(df, acl_type_item):
                                yaml = yaml + self.__generate_yaml_key_only(f'- {acl_type_item}', len_path_split + 3 + level_indent)
                                df_filtered = self.__filter_relevant_aad_oid_and_acl_type(df, acl_aad_oid_item, acl_type_item)

                                group_set = False
                                for index, row in df_filtered.iterrows():
                                    if not group_set:
                                        yaml = yaml + self.__generate_yaml_key_value('name', self.__get_name_for_aad_objectid
                                                                        (row['acl_aad_object_id'], self.__activeDirectory),
                                                                              len_path_split + 5 + level_indent)
                                        group_set = True

                                    if row['acl_level'] == 'access':
                                        yaml = yaml + self.__generate_yaml_key_value('acl_access', row['acl_rights'],
                                                                              len_path_split + 5 + level_indent)
                                    elif row['acl_level'] == 'default':
                                        yaml = yaml + self.__generate_yaml_key_value('acl_default', row['acl_rights'],
                                                                              len_path_split + 5 + level_indent)
                                    else:
                                        self.__logger.warning(f"WRONG ACL VALUE: {row['acl_level']}")

                        if previous_level < len_path_split:
                            yaml = yaml + self.__generate_yaml_key_only('folders', len_path_split + 1 + level_indent)
                            previous_level += 1
                        elif setFolder:
                            yaml = yaml + self.__generate_yaml_key_only('folders', len_path_split + 1 + level_indent)
                        else:
                            self.__logger.debug('YAML key folders already created.')

                        yaml = yaml + '\n'
                else:
                    self.__logger.debug('The path:' + str(path.name) + ' is File. Skipping...')
            else:
                self.__logger.debug(f'The path: {path.name} exceeding Param Max Level {[maxLevel]}. Skipping...')
        self.__logger.debug('------   Writing to a file ---------------')

        output_yaml_git_name = f'acl_{self.__storageName}_{storage_filesystem}.yaml'
        self.__logger.debug(f'Yaml Git path and filename: {self.__configDir}/{export_path}/{output_yaml_git_name}')
        self.__export_yaml(yaml, self.__configDir + '/' + export_path + '/' + output_yaml_git_name)
        self.__logger.info("ACL Export - DONE", extra=info_dict)

    def __add_activeDirectory_users_from_config(self):
        content = """azure:\n  activeDirectory:\n"""
        print(self.__activeDirectory)
        for key, value in self.__activeDirectory.items():
            content = content + "    " + key + ": '" + value + "'\n"
        content = content + '\n'
        return content

    def __export_yaml(self, yaml_text: str, filepath: str):
        yml_file = open(filepath, "wb")
        yml_file.write(yaml_text.encode('utf-8'))
        yml_file.close()

    def __get_available_acl_types(self, df):
        return df['acl_type'].apply(pd.Series).stack().drop_duplicates().tolist()

    def __get_aad_oid_list_per_type(self, df, acl_type):
        df_grouped = df[(df['acl_type'] == acl_type)]
        return df_grouped['acl_aad_object_id'].apply(pd.Series).stack().drop_duplicates().tolist()

    def __filter_relevant_aad_oid_and_acl_type(self, df, acl_aad_oid_item, acl_type_item):
        return df[(df['acl_aad_object_id'] == acl_aad_oid_item) & (df['acl_type'] == acl_type_item)]

    def __generate_yaml_key_only(self, key: str, indent: int):
        return indent * 2 * ' ' + key + ':\n'

    def __generate_yaml_key_value(self, key: str, value: str, indent: int):
        return indent * 2 * ' ' + key + ': ' + value + '\n'

    def __get_folder_properties_from_sdk(self, full_path_object, storage_filesystem):
        self.__logger.debug(f'Full path: {full_path_object.name}')
        full_path = full_path_object.name
        path_splitted = full_path_object.name.split('/')
        len_path_splited = len(path_splitted)
        self.__logger.debug('Filesystem level: ' + str(len_path_splited))
        current_folder = path_splitted[len_path_splited - 1]
        self.__logger.debug(f'Current Folder: {current_folder}')

        file_system_client = self.__serviceClient.get_file_system_client(storage_filesystem)
        directory_client = file_system_client.get_directory_client(full_path)
        acl_props = directory_client.get_access_control()
        self.__logger.debug(acl_props['permissions'])
        self.__logger.debug(acl_props['acl'])

        # Check if the object is a folder or file
        object_properties = directory_client.get_directory_properties()
        self.__logger.debug(object_properties)
        is_folder = 'hdi_isfolder' in object_properties['metadata']
        self.__logger.debug('is_folder: ' + str(is_folder))
        return full_path, current_folder, len_path_splited, acl_props, is_folder

    def __get_name_for_aad_objectid(self, objectid: str, name_oid: dict) -> str:
        for key, value in name_oid.items():
            if value == objectid:
                return "'%azure.activeDirectory." + key + "%'"

        return objectid
