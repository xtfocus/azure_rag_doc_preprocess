# webhook_test_server.py
from fastapi import FastAPI, Request
from loguru import logger

app = FastAPI()


@app.post("/webhook")
async def webhook_endpoint(request: Request):
    data = await request.json()
    logger.info(f"Received webhook: {data}")
    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8009)
