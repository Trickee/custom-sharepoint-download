import os
import datetime
import requests
# import sharepy
import pytz
from sap_business_structure_import.management.commands.bl_utils.sharepy-helper import session as sharepy
from sap_business_structure_import.management.commands.bl_utils.path_generator import PathGenerator
from djangoautoconf.local_key_manager import get_local_key
from dateutil.parser import parse


class DownloadHelper(object):
    def __init__(self, domain_url, site):
        super(DownloadHelper, self).__init__()
        self.domain_url = domain_url
        self.site = site
        self.conn = self.init_connection()

    def init_connection(self):
        username = get_local_key("office365_account.account")
        password = get_local_key("office365_account.app_password")
        s = sharepy.connect(self.domain_url+self.site, username=username, password=password)
        return s

    @classmethod
    def get_all_folders_under_path(cls, domain_url, site, path, conn):
        """
        :param domain_url:
        :param site:
        :param path:
        :param conn:
        :return: folders dictionary-like object
        """
        domain_site_path = {"domain_url": domain_url, "site": site, "path": path}
        folder_url = "{domain_url}{site}/_api/web/GetFolderByServerRelativeUrl('{path}')//Folders".format(
            **domain_site_path)
        response = conn.get(folder_url)
        if cls.is_response_get_success(response):
            folders = response.json()
            return folders['d']['results']

    @classmethod
    def get_all_files_under_folder(cls, domain_url, site, path, conn, folder_name):
        """
        :param path:
        :param folder_name:
        :return:
        """
        domain_site_folder_with_path = {"domain_url": domain_url, "site": site,
                                        "folder_name_under_path": path + "/" + folder_name}
        file_url = "{domain_url}{site}/_api/web/GetFolderByServerRelativeUrl('{folder_name_under_path}')/Files".format(
            **domain_site_folder_with_path)
        response = conn.get(file_url)
        if cls.is_response_get_success(response):
            files = response.json()
            return files['d']['results']

    @classmethod
    def get_file_content_under_folder(cls, domain_url, site, path, conn, folder_name, file_name):
        domain_site_folder_file = {"domain_url": domain_url, "site": site, "path": path,
                                   "folder_name": folder_name,"file_name": file_name}
        file_content_download_url = "{domain_url}{site}/_api/web/GetFileByServerRelativeUrl('{site}/{path}/{folder_name}" \
                       "/{file_name}')/$value".format(**domain_site_folder_file)
        response = conn.get(file_content_download_url)
        if cls.is_response_get_success(response):
            return response

    @classmethod
    def find_latest_obj(cls, array):
        """
        :param self:
        :param folders:
        :return: latest obj url, obj name
        """
        latest = None
        latest_created_time = datetime.datetime(1980, 1, 1).replace(tzinfo=pytz.UTC)
        for obj in array:
            create_time_str = obj["TimeCreated"]
            create_time = parse(create_time_str)
            if create_time > latest_created_time:
                latest_created_time = create_time
                latest = obj['ServerRelativeUrl']
        print latest
        return latest, cls.split_url(latest)

    @staticmethod
    def get_downloaded_file_path(file_name):
        path_g = PathGenerator()
        download_file_path = path_g.get_path_by_current_date(file_name)
        if download_file_path is not None:
            return download_file_path

    @staticmethod
    def is_response_get_success(response):
        return response.status_code == 200

    @staticmethod
    def split_url(url):
        if url is not None:
            return url.split('/')[-1]

    @staticmethod
    def is_file_existed(file_name):
        base_path = "./data/excel"
        year_folders = os.listdir(base_path)
        for year in year_folders:
            year_folder_path = os.path.join(base_path, year)
            month_folders = os.listdir(year_folder_path)
            for month in month_folders:
                month_folder_path = os.path.join(year_folder_path, month)
                all_files_under_month_folder = os.listdir(month_folder_path)
                if file_name in all_files_under_month_folder:
                    return True

    @staticmethod
    def write_download_file_to_local_file(download_filename, download_response):
        is_download_success = False
        try:
            with open(download_filename, 'wb') as f:
                for chunk in download_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        is_download_success = True
        except:
            pass
        return is_download_success

    def down_load_excel_through_latest_folder_and_file(self, path):
        all_folders_under_path = self.get_all_folders_under_path(self.domain_url, self.site, path, self.conn)
        latest_folder_url, folder_name = self.find_latest_obj(all_folders_under_path)
        if latest_folder_url and folder_name:
            all_files_under_folder = self.get_all_files_under_folder(self.domain_url, self.site, path,
                                                                     self.conn, folder_name)
            latest_file_url, latest_file_name = self.find_latest_obj(all_files_under_folder)
            if latest_file_name:
                download_filename_path = self.get_downloaded_file_path(latest_file_name)
                file_content = self.get_file_content_under_folder(self.domain_url, self.site, path,
                                                                  self.conn, folder_name, latest_file_name)
                is_file_download_success = False
                if not self.is_file_existed(latest_file_name):
                    if self.write_download_file_to_local_file(download_filename_path, file_content):
                        is_file_download_success = True
                return is_file_download_success, download_filename_path






