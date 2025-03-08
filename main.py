import asyncio
import json
import logging
import os

import aiohttp
import redis
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

load_dotenv()
logger = logging.getLogger(__name__)

try:
    r = redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=os.getenv("REDIS_PORT"),
        password=os.getenv("REDIS_PASS"),
    )
    r.ping()
except redis.ConnectionError as e:
    logger.error(f"Fail to connect to redis: {e}")


limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/{location}")
@limiter.limit("5/minute")
async def getWeather(location: str, request: Request):
    key = location.lower()
    try:
        data = r.get(key)
        if data:
            return json.loads(data)
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://api.seniverse.com/v3/weather/now.json?key={os.getenv('PRIV_KEY')}&location={location}",
            ) as res:
                logger.info(
                    f"request completed for {request.client.host}:{request.client.port}"
                )
                result = await res.text(encoding="utf-8")
                r.setex(key, 300, result)
                return json.loads(result)
    except redis.RedisError as e:
        logger.error(f"Redis error:{e}")
    except aiohttp.ClientResponseError as e:
        logger.error(
            f"A reponse error for {request.client.host}:{request.client.port}: {e}"
        )
    except aiohttp.ClientConnectionError as e:
        logger.error(
            f"Connection error for {request.client.host}:{request.client.port}: {e}"
        )
    except aiohttp.RedirectClientError as e:
        logger.error(
            f"Redirect error for {request.client.host}:{request.client.port}: {e}"
        )
    except Exception as e:
        logger.error(
            f"Other errors for {request.client.host}:{request.client.port}: {e}"
        )

    return {"status": "Some error occured"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        reload=True,
        port=8000,
    )
