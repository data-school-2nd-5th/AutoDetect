from azure.storage.blob import  ContainerClient
from .get_env import get_env

class AzureBlobUploader:
    def __init__(self, sas_url: str, sas_token: str):
        # self.base_url = sas_url
        # self.sas_token = sas_token
        # self.full_url = f"{self.base_url}?{self.sas_token}"
        # self.container_client = ContainerClient.from_container_url(self.full_url)
        # container_properties = self.container_client.get_container_properties()  # 연결 테스트
        # print(f"컨테이너 연결 성공: {container_properties.name}")
        pass

    def upload_text(self, file_name: str, content: str):
        """
        플레인 텍스트를 지정한 파일 이름으로 업로드합니다.
        """
        try:
            blob_client = self.container_client.get_blob_client(file_name)
            print(f"업로드 시작: {file_name}")
            blob_client.upload_blob(content, overwrite=True)
            print(f"업로드 성공: {file_name}")
            
        except Exception as e:
            print(f"업로드 실패: {e}")
            raise
    def upload_blob(self, file_name: str, code_text: str):
        """
        텍스트 데이터를 지정한 파일 이름으로 업로드합니다.
        """
        try:
            blob_client = self.container_client.get_blob_client(file_name)
            print(f"업로드 시작: {file_name}")
            blob_client.upload_blob(code_text, overwrite=True)
            print(f"업로드 성공: {file_name}")
            
        except Exception as e:
            print(f"업로드 실패: {e}")
            raise e

url = get_env("JS_SAS_URL")  # 환경 변수로 설정 (선택 사항)
token = get_env("JS_SAS_TOKEN")  # 환경 변수로 설정 (선택 사항)

azure_blob_uploader = AzureBlobUploader(url, token)