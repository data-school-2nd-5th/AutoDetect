import os
from typing import overload, Union, Optional, Literal, Any

class MissingEnvironmentVariableError(Exception):
    """환경 변수가 설정되지 않았을 때 발생하는 커스텀 예외"""
    def __init__(self, key: str):
        self.message = f"필수 환경 변수 '{key}'가 설정되지 않았습니다."
        super().__init__(self.message)

# 1. 예외를 발생하는 경우: 반환 타입은 무조건 str (Optional 제거)
@overload
def get_env(
    key: str, 
    default: Optional[str] = None, 
    raise_exception: Literal[True] = True
) -> str: ...

# 2. 예외를 발생시키지 않는 경우: 반환 타입은 str 또는 None (Optional[str])
@overload
def get_env(
    key: str, 
    default: Optional[str] = None, 
    raise_exception: Literal[False] = False
) -> Optional[str]: ...

# 3. 실제 실행 로직 (Implementation)
def get_env(key: str, default: Optional[str] = None, raise_exception: bool = True) -> Any:
    """
    환경 변수를 가져옵니다. 
    값이 없고 raise_exception이 True인 경우 예외를 발생시킵니다. (기본값: True)
    """
    value = os.getenv(key, default)
    
    if value is not None:
        return value
        
    if not raise_exception:
        return default
        
    raise MissingEnvironmentVariableError(key)