from dagster import resource
from openai import OpenAI

@resource(config_schema={"api_key": str, "model": str})
def openai_client(context):
    client = OpenAI(api_key=context.resource_config["api_key"])
    model = context.resource_config["model"]

    def query_llm(prompt: str) -> str:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a performance analyst for data engineering benchmarks."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.4
        )
        return response.choices[0].message.content

    return query_llm