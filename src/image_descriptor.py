from typing import Any

from loguru import logger
from openai import AsyncAzureOpenAI


class ImageDescriptor:
    """
    Decribe an image
    """

    def __init__(self, client: AsyncAzureOpenAI, config: Any, prompt: str):
        self.client = client
        self.config = config
        self.prompt = prompt

    async def run(self, base64_data: str, summary: str, temperature=None) -> str:
        """
        base64_data: base64 str
        """
        if not temperature:
            temperature = self.config.temperature

        try:
            logger.debug("create image description request")
            response = await self.client.chat.completions.create(
                model=self.config.MODEL_DEPLOYMENT,
                temperature=temperature,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": self.prompt,
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_data}"
                                },
                            },
                            {
                                "type": "text",
                                "text": f"For context, the image above is extracted from  a document having description as follows: {summary}",
                            },
                        ],
                    }
                ],
            )
        except Exception as e:
            logger.error(str(e))
            # with open("error_image.txt", "w") as h:
            #     h.write(base64_data)
            raise

        logger.debug("finish image description request")
        return response.choices[0].message.content
