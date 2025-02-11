from typing import Any, Literal

from openai import AsyncAzureOpenAI
from pydantic import BaseModel


class ImageDescription(BaseModel):
    image_type: Literal["icon", "shape", "logo", "picture", "information"]
    image_description: str


class ImageDescriptor:
    """
    Decribe an image
    """

    def __init__(self, client: AsyncAzureOpenAI, config: Any, prompt: str):
        self.client = client
        self.config = config
        self.prompt = prompt

    async def run(self, base64_data: str, summary: str, temperature=None):
        """
        base64_data: base64 str
        """
        if not temperature:
            temperature = self.config.temperature

        response = await self.client.beta.chat.completions.parse(
            model=self.config.MODEL_DEPLOYMENT,
            response_format=ImageDescription,
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

        # Parse response
        data = response.choices[0].message.parsed
        return data
