from typing import Any

from openai import AsyncAzureOpenAI


class ImageDescriptor:
    def __init__(self, client: AsyncAzureOpenAI, config: Any, prompt: str):
        self.client = client
        self.config = config
        self.prompt = prompt

    async def run(self, base64_data, temperature=None):
        if not temperature:
            temperature = self.config.temperature

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
                    ],
                }
            ],
        )
        return response.choices[0].message.content