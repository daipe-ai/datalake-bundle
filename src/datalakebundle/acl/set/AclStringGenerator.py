# pylint: disable = invalid-name
from logging import Logger
import pandas as pd

class AclStringGenerator:

    def __init__(
        self,
        logger: Logger,
    ):
        self.__logger = logger

    def create_folder_properties_pd_table(self, acl_value):
        acl_list = acl_value.split(',')
        self.__logger.debug(acl_list)
        df = pd.DataFrame({'Nested_rights': acl_list})

        # normalizing the rights names, to correctly split into columns
        df['Nested_rights'] = df['Nested_rights'].str.replace('^user:', 'access:user:')
        df['Nested_rights'] = df['Nested_rights'].str.replace('^group:', 'access:group:')
        df['Nested_rights'] = df['Nested_rights'].str.replace('^mask:', 'access:mask:')
        df['Nested_rights'] = df['Nested_rights'].str.replace('^other:', 'access:other:')
        df['Nested_rights'] = df['Nested_rights'].str.replace('::', ':OWNER:')

        # Split to columns
        # new data frame with split value columns
        splited_df = df["Nested_rights"].str.split(":", n=3, expand=True)
        df = df.drop(columns=['Nested_rights'])

        # making separate first name column from new data frame
        df["acl_level"] = splited_df[0]
        df["acl_type"] = splited_df[1]
        df["acl_aad_object_id"] = splited_df[2]
        df["acl_rights"] = splited_df[3]

        return df

    def create_acl_string(self, acl_config: dict):
        acl_string = f""  # Start of the ACL string
        first_run = True
        for acl_type_item in acl_config:
            self.__logger.debug(acl_type_item)
            for aad_oid_master in acl_config[acl_type_item]:
                self.__logger.debug(aad_oid_master)
                for acl_aad_oid_item in aad_oid_master:
                    self.__logger.debug(acl_aad_oid_item)  # user, group, mask
                    name = aad_oid_master[acl_aad_oid_item]['name']
                    if 'acl_access' in aad_oid_master[acl_aad_oid_item]:
                        acl_access = aad_oid_master[acl_aad_oid_item]['acl_access']
                    else:
                        acl_access = '---'

                    if 'acl_default' in aad_oid_master[acl_aad_oid_item]:
                        acl_default = aad_oid_master[acl_aad_oid_item]['acl_default']
                    else:
                        acl_default = '---'

                    # add comma if not a first run
                    if not first_run:
                        acl_string = acl_string + ','

                    if name == 'OWNER':
                        acl_string = acl_string + f'{acl_aad_oid_item}::{acl_access},default:{acl_aad_oid_item}::{acl_default}'
                    else:
                        acl_string = acl_string + f'{acl_aad_oid_item}:{name}:{acl_access},default:{acl_aad_oid_item}:{name}:{acl_default}'
                    first_run = False
                    # (acl='user::rwx,group:e942240a-4ada-4c45-aec6-7a8ef65134b9:rwx,group::r-x,mask::rwx,other::---,default:user::rwx,default:user:48910f42-9910-4078-bc2c-f4933
                    self.__logger.debug('Temporary ACL: ' + acl_string)
                    self.__logger.debug(10 * '_')
        self.__logger.debug(acl_string)
        return acl_string

    def create_acl_for_file(self, acl_string: str):
        def listToString(s):
            str1 = ""
            i = 0
            # traverse in the string
            for ele in s:
                i += 1
                if i != len(s):
                    str1 += ele + ','
                else:
                    str1 += ele
            return str1

        acl_list = acl_string.split(',')
        self.__logger.debug(acl_list)
        acl_list_new = []
        for acl in acl_list:
            if acl.find(r'default:') != 0:
                acl_list_new.append(acl)
        return listToString(acl_list_new)
