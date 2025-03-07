import json
import logging
import os

import redis
import requests
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
    print("redis connected")
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
            return data
    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")

    try:
        res = requests.get(
            f"https://api.seniverse.com/v3/weather/now.json?key={os.getenv('PRIV_KEY')}&location={location}"
        )
        logger.info(
            f"request completed for {request.client.host}:{request.client.port}"
        )
        toCache = res.text
        result = json.loads(toCache)
        r.setex(key, 3000, toCache)
    except redis.RedisError as e:
        logger.error(f"Redis error:{e}")
    except requests.ConnectionError as e:
        logger.error(
            f"An connection error for {request.client.host}:{request.client.port}: {e}"
        )
    except requests.HTTPError as e:
        logger.error(f"HTTP error for {request.client.host}:{request.client.port}: {e}")
    except requests.TooManyRedirects as e:
        logger.error(
            f"Too many redirects for {request.client.host}:{request.client.port}: {e}"
        )
    except Exception as e:
        logger.error(
            f"Other errors for {request.client.host}:{request.client.port}: {e}"
        )

    return result


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        reload=True,
        port=8000,
    )
