# pylint: disable = invalid-name
import os
from logging import Logger

class AclPathResolver:
    def __init__(
        self,
        logger: Logger,
    ):
        self.__logger = logger

    def get_path_and_filename(self, storage_path: str):
        filename = os.path.basename(storage_path)
        path_to_file = os.path.dirname(storage_path)
        return path_to_file, filename

    def get_filesystem_and_path_from_root(self, folder_name: str, root_path_current_folder: str):
        full_path_folder = root_path_current_folder + '/' + folder_name
        self.__logger.debug('Full Path to current folder: ' + full_path_folder)
        # Set the ACL to current folder
        filesystem = full_path_folder.split('/')[1]
        self.__logger.debug(f'Filesystem: {filesystem}')
        full_path_folder_to_set_acl = full_path_folder[len(filesystem) + 1:]  # skipping the filesystem name and "/"
        self.__logger.debug(f'Path without filesystem to set ACL with SDK: {full_path_folder_to_set_acl}')
        return full_path_folder, filesystem, full_path_folder_to_set_acl
