from pydantic import BaseModel, field_validator
from pydantic.dataclasses import dataclass
from typing import Union, List


@dataclass
class Account:
    name: str
    mode: str = 'demo'
    secret: str = ''

    def __str__(self):
        return f'{self.__class__.__name__}({self.name}, {self.mode})'

    def __repr__(self):
        return self.__str__()


@dataclass
class Param:
    account: Account
    symbols: List[str]
    timeframe: int
    breaker: bool = False
    volume: float = 0.01
    rate_tp: Union[float, int] = 0
    rate_sl: Union[float, int] = 0
    indicator: str = 'rsi'
    ind_preset: str = ''

    @field_validator('account', mode='before')
    def account_post_init(cls, v):
        if isinstance(v, str):
            v = Account(v)
        return v


class Profile(BaseModel):
    name: str
    param: Param

    @field_validator('param', mode='before')
    def param_post_init(cls, v):
        if isinstance(v, dict):
            v = Param(**v)
        return v


class Settings(BaseModel):
    rayId: str
    _comment: str
    profiles: List[Profile]

    @field_validator('profiles', mode='before')
    def profiles_post_init(cls, v):
        if isinstance(v, list):
            v = [Profile(**p) for p in v]
            v.sort(key=lambda x: x.param.account.name, reverse=True)
        return v
