# pylint: disable = invalid-name, too-many-locals, too-many-branches, too-many-statements, too-many-nested-blocks
from logging import Logger
from azure.core.exceptions import HttpResponseError
from azure.storage.filedatalake import DataLakeServiceClient
from datalakebundle.acl.AclConfigReader import AclConfigReader
from datalakebundle.acl.AclPathResolver import AclPathResolver
from datalakebundle.acl.set.AclStringGenerator import AclStringGenerator

class AclSetter:

    def __init__(
        self,
        configDir: str,
        storageName: str,
        logger: Logger,
        serviceClient: DataLakeServiceClient,
        aclConfigReader: AclConfigReader,
        aclStringGenerator: AclStringGenerator,
        aclPathResolver: AclPathResolver
    ):
        self.__configDir = configDir
        self.__storageName = storageName
        self.__logger = logger
        self.__serviceClient = serviceClient
        self.__aclConfigReader = aclConfigReader
        self.__aclStringGenerator = aclStringGenerator
        self.__aclPathResolver = aclPathResolver

    def set(self, storage_filesystem, export_path):
        info_dict = {
            'storage_account': self.__storageName,
            'storage_filesystem': storage_filesystem,
            'export_path': export_path
        }
        self.__logger.info('Acl Set started.', extra=info_dict)

        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)
        paths = file_system_client.get_paths()
        self.__logger.debug('get_paths() initialized', extra={'path': paths})

        # Static name for Git
        output_yaml_git_name = f'acl_{self.__storageName}_{storage_filesystem}.yaml'
        yaml_path = self.__configDir + '/' + export_path + '/' + output_yaml_git_name
        config = self.__aclConfigReader.read_config(yaml_path)

        self.__logger.debug('Iterates over Filesystem first level')
        for folder_name, folder_config in config.to_dict().items():
            if folder_name not in ('storage_account', 'azure'):
                self.__set_acl_for_dir(folder_name, folder_config, '')

        self.__logger.info('Acl Set Done.', extra=info_dict)

    # Recursive function to iterate via entire YAML
    def __set_acl_for_dir(self, folder_name: str, folder_config: dict, path_current_folder: str):
        # set real ACL
        self.__logger.debug(f'Iterates over Folder {folder_name} with root path: {path_current_folder}')
        self.__set_acl_for_dir_real(folder_name, folder_config, path_current_folder)

        # Iterates over all folders and subfolders
        if 'folders' in folder_config:
            if folder_config['folders'] is not None:
                for subfolder_name, subfolder_config in folder_config['folders'].items():
                    self.__set_acl_for_dir(subfolder_name, subfolder_config, path_current_folder + '/' + folder_name)
            else:
                # Following code setts the Default rights to inherited folder
                self.__logger.debug('Setting ACL to the inherited folder: ' + str(path_current_folder) + '/' + folder_name)
                full_path_folder, filesystem, subfolder = self.__aclPathResolver.get_filesystem_and_path_from_root(folder_name,
                                                                                              path_current_folder)
                self.__logger.debug(f'full_path_folder: {full_path_folder}')

                file_system_client = self.__serviceClient.get_file_system_client(file_system=filesystem)
                paths = file_system_client.get_paths(subfolder)

                # Get Rights to be appliead to the inherited folders
                if subfolder == '':
                    subfolder = '.'
                self.__logger.debug(f'Getting Rights to be appliead to the inherited folders for subfolder: {subfolder}')
                sub_directory_client = file_system_client.get_directory_client(subfolder)
                sub_acl_props = sub_directory_client.get_access_control()

                # pd with only deffault values to inherit
                self.__logger.debug(f'Getting only Default ACL from last folder in YAML')
                df = self.__aclStringGenerator.create_folder_properties_pd_table(sub_acl_props['acl'])
                is_default = df['acl_level'] != 'access'
                df_default = df[is_default]
                first_run = True
                acl_string = ''
                self.__logger.debug(f'Creating ACL string')
                for index, row in df_default.iterrows():
                    self.__logger.debug(f'index: {index}')
                    acl_aad_oid_item = row['acl_aad_object_id']
                    acl_type = row['acl_type']
                    acl_access = row['acl_rights']
                    acl_default = row['acl_rights']

                    # add comma if not a first run
                    if acl_default != '---':  # only create ACL for users that have some ACL Default defined
                        if not first_run:
                            acl_string = acl_string + ','
                        if acl_aad_oid_item == 'OWNER':
                            acl_string = acl_string + f'{acl_type}::{acl_access},default:{acl_type}::{acl_default}'
                        else:
                            acl_string = acl_string + f'{acl_type}:{acl_aad_oid_item}:{acl_access},' \
                                                                f'default:{acl_type}:{acl_aad_oid_item}:{acl_default}'
                        first_run = False
                self.__logger.debug(f'ACL String: {acl_string}')

                self.__logger.debug(100 * '*')
                for path in paths:
                    self.__logger.debug(20 * '_')
                    self.__logger.debug(f' -- Setting ACL to subdirectories of {subfolder}')
                    self.__logger.debug(path.name)
                    self.__set_acl(filesystem, path.name, acl_string)

    def __set_acl_for_dir_real(self, folder_name: str, folder_config: dict, root_path_current_folder: str):
        self.__logger.debug('-------------------------------')
        full_path_folder, filesystem, full_path_folder_to_set_acl = self.__aclPathResolver.get_filesystem_and_path_from_root(folder_name,
                                                                                                         root_path_current_folder)
        self.__logger.debug('folder: {}'.format(folder_name))
        acl_config = folder_config['acl']
        self.__logger.debug(acl_config)

        acl_string = self.__aclStringGenerator.create_acl_string(acl_config)

        # In case it is a filesystem the path is replaced by '.'
        if full_path_folder_to_set_acl == '':
            full_path_folder_to_set_acl = ''
        self.__set_acl(filesystem, full_path_folder_to_set_acl, acl_string)
        self.__logger.debug('Set ACL to the files in current folder.')

        self.__logger.debug(f'set_acl_for_subfolders_and_files({filesystem}, {full_path_folder}, {acl_string})')
        self.__set_acl_for_subfolders_and_files(filesystem, full_path_folder[len(filesystem) + 1:], acl_string)

    def __set_acl(self, storage_filesystem, storage_path, acl_string):
        self.__logger.debug(f'{storage_filesystem}, {storage_path}, {acl_string}')
        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)
        directory_client = file_system_client.get_directory_client(storage_path)
        try:
            directory_client.set_access_control(acl=acl_string)
        except HttpResponseError as e:
            if e.error_code != 'DefaultAclOnFileNotAllowed': # pylint: disable = no-member
                raise

            self.__logger.debug(5 * '-*')
            self.__logger.debug(storage_path)
            path_to_file, filename = self.__aclPathResolver.get_path_and_filename(storage_path)
            self.__logger.debug(f'Setting ACL to File: {path_to_file}/{filename}')
            self.__logger.debug(filename)
            self.__logger.debug(path_to_file)
            if path_to_file == '':
                path_to_file = ''
            directory_client = file_system_client.get_directory_client(path_to_file)
            file_client = directory_client.get_file_client(filename)
            file_acl_string = self.__aclStringGenerator.create_acl_for_file(acl_string)
            file_client.set_access_control(acl=file_acl_string)


    def __set_acl_for_subfolders_and_files(self, storage_filesystem: str, folder: str, acl_string: str):
        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)

        sub_paths = file_system_client.get_paths(folder, recursive=False)
        for folder_file in sub_paths:
            self.__logger.debug(folder_file.name)
            self.__set_acl(storage_filesystem, folder_file.name, acl_string)
