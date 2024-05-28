import logging
from openai import OpenAI

from tenacity import before_sleep_log, retry, stop_after_attempt, wait_fixed

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

my_open_api_key = "sk-proj-KOa1AD7l1MvbLpghaBomT3BlbkFJvh4QgJLtPr4xK6OidOm4"
client = OpenAI(api_key=my_open_api_key)

@retry(
    wait=wait_fixed(2),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def get_response(prompt):
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt}
        ]
        )

    return completion.choices[0].message.content
