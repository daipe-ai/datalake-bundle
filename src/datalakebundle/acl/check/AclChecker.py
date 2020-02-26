# pylint: disable = invalid-name, too-many-locals, too-many-branches, too-many-statements, too-many-nested-blocks
from logging import Logger
import sys
from azure.core.exceptions import HttpResponseError
from azure.storage.filedatalake import DataLakeServiceClient
from datalakebundle.acl.AclConfigReader import AclConfigReader
from datalakebundle.acl.AclPathResolver import AclPathResolver
from datalakebundle.acl.set.AclStringGenerator import AclStringGenerator

class AclChecker:

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

    def check(self, storage_filesystem, export_path):
        self.__logger.debug('---------- Init Parameters   ------------------------')

        self.__logger.debug('Init Connection')
        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)
        self.__logger.debug('aclInitClient initialized')
        paths = file_system_client.get_paths()
        self.__logger.debug('get_paths() initialized', extra={'path': paths})

        # Static name for Git
        output_yaml_git_name = f'acl_{self.__storageName}_{storage_filesystem}.yaml'
        yaml_path = self.__configDir + '/' + export_path + '/' + output_yaml_git_name
        # Read Config in YAML
        config = self.__aclConfigReader.read_config(yaml_path)

        ### Iterates only over Filesystem first level -> than recursive via set_acl_for_dir()
        self.__logger.debug('Iterates only over Filesystem first level')
        for folder_name, folder_config in config.to_dict().items():
            if folder_name not in ('storage_account', 'azure'):
                self.__check_acl_for_dir(folder_name, folder_config, '')

    # Recursive function to iterate via entire YAML
    def __check_acl_for_dir(self, folder_name: str, folder_config: dict, path_current_folder: str):
        # set real ACL
        subfolders_lists = []
        self.__logger.debug(f'Iterates only over Folder {folder_name} with root path: {path_current_folder}')
        self.__check_acl_for_dir_real(folder_name, folder_config, path_current_folder)

        # Iterates over all folders and subfolders
        if 'folders' in folder_config:
            if folder_config['folders'] is not None:
                for subfolder_name, subfolder_config in folder_config['folders'].items():
                    self.__check_acl_for_dir(subfolder_name, subfolder_config, path_current_folder + '/' + folder_name)
                    subfolders_lists.append(subfolder_name)
            else:
                # Following code setts the Default rights to inherited folder
                self.__logger.debug('Setting ACL to the inherited folder: ' + str(path_current_folder) + '/' + folder_name)
                full_path_folder, filesystem, subfolder = self.__aclPathResolver.get_filesystem_and_path_from_root(folder_name,
                                                                                              path_current_folder)
                self.__logger.debug(f'full_path_folder: {full_path_folder}')
                file_system_client = self.__serviceClient.get_file_system_client(file_system=filesystem)

                # Get Rights to be applied to the inherited folders
                if subfolder == '':
                    subfolder = '.'
                self.__logger.debug(f'Getting Rights to be appliead to the inherited folders for subfolder: {subfolder}')
                sub_directory_client = file_system_client.get_directory_client(subfolder)
                sub_acl_props = sub_directory_client.get_access_control()

                # pd with only default values to inherit
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
                            acl_string = acl_string + f'{acl_type}:{acl_aad_oid_item}:{acl_access},default:{acl_type}:{acl_aad_oid_item}:{acl_default}'
                        _first_run = False
                self.__logger.debug(f'ACL String: {acl_string}')

                self.__logger.debug(100 * '*')

            # Check if there are more folders in storage than in Config
            subfolders_lists_real = []
            if subfolders_lists != []:
                self.__logger.debug('Check if there are more folders in storage')
                full_path_folder, filesystem, subfolder = self.__aclPathResolver.get_filesystem_and_path_from_root(folder_name,
                                                                                                     path_current_folder)
                file_system_client_check = self.__serviceClient.get_file_system_client(file_system=filesystem)

                if subfolder == '':
                    # filesystem level
                    subfolder = '/'

                sub_paths = file_system_client_check.get_paths(subfolder, recursive=False)
                for folder_file in sub_paths:

                    directory_client = file_system_client_check.get_directory_client(folder_file.name)
                    object_properties = directory_client.get_directory_properties()
                    if 'hdi_isfolder' in object_properties['metadata']:
                        subfolders_lists_real.append(folder_file.name)

                if len(subfolders_lists) != len(subfolders_lists_real):
                    self.__logger.warning(
                        f"WARN: Folders structure DOES NOT match folders in YAML Config in path {subfolder}."
                        f"\n - YAML Folders Config: {subfolders_lists}"
                        f"\n - Folders currently in storage: {subfolders_lists_real}"
                        f"\n Please Check or run Structure manager and ACL Export")

    def __check_acl_in_set(self, acl_active_list: str, acl_to_find: str):
        for acl_active in acl_active_list.split(','):
            if acl_to_find == acl_active:
                return True
        return False

    def __check_acl(self, storage_filesystem, storage_path, acl_string, checkFilesOnly):
        self.__logger.debug(f'Checking ACL for FS: {storage_filesystem}, Path: {storage_path}')
        file_system_client_checker = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)

        directory_client_checker = file_system_client_checker.get_directory_client(storage_path)
        acl_active = directory_client_checker.get_access_control()
        self.__logger.debug(f"ACL YAML Config: {acl_string}")
        self.__logger.debug(f"ACL currently set is: {acl_active['acl']}")

        # Compare ACL Active and in YAML
        # Check if the object is a folder or file
        if storage_path != '.':
            object_properties = directory_client_checker.get_directory_properties()
            is_folder = 'hdi_isfolder' in object_properties['metadata']
            self.__logger.debug(object_properties)
        else:
            is_folder = True

        self.__logger.debug(f'Is Folder(or FS): {is_folder}')
        if is_folder:
            # FOLDER
            if len(acl_string) != len(acl_active['acl']):
                self.__logger.warning(
                    f"WARN: ACL in YAML Config DOES NOT match for FOLDER `{storage_path}`. \n - YAML ACL Config: `{acl_string}`\n - ACL currently set for folder is: **{acl_active['acl']}**")
            else:
                if not checkFilesOnly:
                    for acl_yaml in acl_string.split(','):
                        # print(f'Checking if ACL string: `{acl_yaml}` is in Active ACL.')
                        if not self.__check_acl_in_set(acl_active['acl'], acl_yaml):
                            self.__logger.warning(
                                f"WARN: ACL in YAML Config `{acl_yaml}` DOES NOT match for FOLDER `{storage_path}`. ACL currently set for folder is: **{acl_active['acl']}**")
                        else:
                            self.__logger.debug(f"OK: Defined ACL `{acl_yaml}` is already correctly set.")

        else:
            # FILE
            self.__logger.debug(storage_path)
            _path_to_file, _filename = self.__aclPathResolver.get_path_and_filename(storage_path)
            self.__logger.debug(f'Checking ACL of File: {_path_to_file}/{_filename}')
            self.__logger.debug(f'_filename: {_filename}')
            if _path_to_file == '':
                _path_to_file = ''
            self.__logger.debug(f'_path_to_file: {_path_to_file}')
            directory_client = file_system_client_checker.get_directory_client(_path_to_file)
            file_client = directory_client.get_file_client(_filename)
            acl_active = file_client.get_access_control()  # checking
            file_acl_string = self.__aclStringGenerator.create_acl_for_file(acl_string)

            for acl_yaml in file_acl_string.split(','):
                self.__logger.debug(f'Checking if ACL string from YAML Config: `{acl_yaml}` is set in Active ACL.')
                self.__logger.debug('acl test: ' + str(acl_active['acl'] + ' acl_yaml: ' + str(acl_yaml)))
                if not self.__check_acl_in_set(acl_active['acl'], acl_yaml):
                    self.__logger.warning(
                        f"Defined ACL `{acl_yaml}` doesn`t match active ACL in for FILE `{_path_to_file}{_filename}`. Active folder ACL to check: **{acl_active['acl']}**")
                else:
                    self.__logger.debug(f"OK: Defined ACL `{acl_yaml}` is already correctly set.")

    def __check_acl_for_dir_real(self, folder_name: str, folder_config: dict, root_path_current_folder: str):
        self.__logger.debug('-------------------------------')
        _full_path_folder, _filesystem, _full_path_folder_to_set_acl = self.__aclPathResolver.get_filesystem_and_path_from_root(folder_name,
                                                                                                         root_path_current_folder)
        self.__logger.debug('folder: {}'.format(folder_name))
        acl_config = folder_config['acl']
        self.__logger.debug(acl_config)
        # Create ACL string
        acl_string = self.__aclStringGenerator.create_acl_string(acl_config)

        # In case it is a filesystem the path is replaced by '.'
        if _full_path_folder_to_set_acl == '':
            _full_path_folder_to_set_acl = ''

        self.__logger.debug('-------------Check_rbac ------------------')
        self.__check_acl(_filesystem, _full_path_folder_to_set_acl, acl_string, checkFilesOnly=False)

    def __check_acl_orchestrator(self, storage_filesystem, storage_path, acl_string):
        self.__logger.debug(f'{storage_filesystem}, {storage_path}, {acl_string}')
        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)
        directory_client = file_system_client.get_directory_client(storage_path)
        try:
            directory_client.set_access_control(acl=acl_string)
        except HttpResponseError as e:
            if e.error_code != 'DefaultAclOnFileNotAllowed':  # pylint: disable = no-member
                self.__logger.debug(5 * '-*')
                self.__logger.debug(storage_path)
                _path_to_file, _filename = self.__aclPathResolver.get_path_and_filename(storage_path)
                self.__logger.debug(f'Setting ACL to File: {_path_to_file}/{_filename}')
                self.__logger.debug(_filename)
                self.__logger.debug(_path_to_file)
                if _path_to_file == '':
                    _path_to_file = ''
                directory_client = file_system_client.get_directory_client(_path_to_file)
                file_client = directory_client.get_file_client(_filename)
                file_acl_string = self.__aclStringGenerator.create_acl_for_file(acl_string)
                file_client.set_access_control(acl=file_acl_string)
            else:
                self.__logger.error(str(e))
                sys.exit(e)

    def __check_acl_for_subfolders_and_files(self, storage_filesystem: str, folder: str, acl_string: str):
        file_system_client = self.__serviceClient.get_file_system_client(file_system=storage_filesystem)

        sub_paths = file_system_client.get_paths(folder, recursive=False)
        for folder_file in sub_paths:
            self.__logger.debug(folder_file.name)
            self.__check_acl_orchestrator(storage_filesystem, folder_file.name, acl_string)
