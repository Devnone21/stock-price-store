import os
import json
from redis.client import Redis


class Cache:
    """Extended class of Redis cache. Accept dict and store as string."""
    def __init__(self) -> None:
        self.ttl_s: int = 604_800
        self.client: Redis = Redis(
            host=os.getenv("REDIS_HOST"),
            port=os.getenv("REDIS_PORT"),
            decode_responses=True,
        )

    def set_key(self, key: str, value: dict, **kwargs):
        self.ttl_s = kwargs.pop('ttl_s', self.ttl_s)
        self.client.set(key, json.dumps(value), ex=self.ttl_s)

    def get_key(self, key: str) -> dict:
        return json.loads(self.client.get(key))

    def get_keys(self, keys: list[str]) -> list[dict]:
        return [json.loads(s) for s in self.client.mget(keys)]
