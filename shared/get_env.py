import os
from typing import Optional

class MissingEnvironmentVariableError(Exception):
    """환경 변수가 설정되지 않았을 때 발생하는 커스텀 예외"""
    def __init__(self, key: str):
        self.message = f"필수 환경 변수 '{key}'가 설정되지 않았습니다."
        super().__init__(self.message)

def get_env(key: str, default: Optional[str] = None, no_raise_exception: bool = False) -> Optional[str]:
    """
    환경 변수를 가져옵니다. 
    값이 없고 no_raise_exception이 False인 경우 예외를 발생시킵니다.
    """
    value = os.getenv(key, default)
    
    if value is not None:
        return value
        
    if no_raise_exception:
        return default
        
    raise MissingEnvironmentVariableError(key)