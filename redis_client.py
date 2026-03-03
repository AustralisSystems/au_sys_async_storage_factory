import logging
import urllib.parse
from typing import Any, Optional

try:
    from redis.asyncio import ConnectionPool, Redis
    from redis.asyncio.cluster import RedisCluster
except ImportError:
    ConnectionPool = Any
    Redis = Any
    RedisCluster = Any

logger = logging.getLogger("storage.redis_client")


class StorageRedisFactory:
    """
    Unified Redis connection manager for the Storage Factory.
    Implements dynamic submodule resolution (Storage Factory Symbiosis) and
    a robust built-in standalone fallback connection pool.
    """

    _pool: Optional[ConnectionPool] = None

    @classmethod
    def get_client(
        cls,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        ssl: bool = False,
        use_cluster: bool = False,
        **kwargs: Any,
    ) -> Redis | RedisCluster:
        """
        Dynamically resolve the Redis client.
        Prioritizes the standalone `redis` submodule if available.
        Falls back to an internal connection pool if absent.
        """
        if use_cluster:
            # Standalone submodule typically doesn't wrap RedisCluster transparently in shared_redis_client,
            # or if it does, it's behind the same interface. We'll handle cluster natively here if requested.
            pass
        else:
            try:
                # Attempt to import from the standalone submodule
                from redis.client import shared_redis_client  # type: ignore

                if hasattr(shared_redis_client, "client"):
                    logger.info("Storage Factory Symbiosis: Utilizing agentic_code_engine Redis sub-module client.")
                    return shared_redis_client.client
            except ImportError:
                # This naturally catches the case where pip's `redis.client` is loaded
                # and doesn't have `shared_redis_client`.
                pass
            except Exception as e:
                logger.warning(f"Error while resolving standalone Redis sub-module: {e}")

        # Fallback to internal connection pool
        logger.debug(
            "Storage Factory Symbiosis: Redis sub-module not found or cluster requested. Utilizing internal fallback pool."
        )

        url = f"rediss://{host}:{port}" if ssl else f"redis://{host}:{port}"
        if password:
            encoded_password = urllib.parse.quote(password)
            url = url.replace("://", f"://:{encoded_password}@")

        if use_cluster:
            # Cluster mode does not use standard ConnectionPool in the same way
            return RedisCluster.from_url(url, **kwargs)

        if cls._pool is None:
            logger.info(f"Initializing Storage Factory internal Redis fallback pool for {host}:{port}")
            cls._pool = ConnectionPool.from_url(url, max_connections=50, decode_responses=True, **kwargs)
        return Redis(connection_pool=cls._pool)

    @classmethod
    async def close_pool(cls) -> None:
        """
        Gracefully close the internal connection pool during shutdown.
        """
        if cls._pool is not None:
            logger.info("Closing Storage Factory internal Redis fallback pool")
            await cls._pool.disconnect()
            cls._pool = None
