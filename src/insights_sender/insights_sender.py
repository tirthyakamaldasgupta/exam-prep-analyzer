from datetime import datetime
import io
import json
import os
import zipfile
import boto3
from dotenv import load_dotenv
from redmail import gmail
from PIL import Image

load_dotenv()

gmail.username = os.environ["EMAIL_USERNAME"]
gmail.password = os.environ["EMAIL_PASSWORD"]
gmail.set_template_paths(html=".")


def handler(event, context):
    s3 = boto3.resource("s3")

    compressed_insights = s3.Object(
        event["Records"][0]["s3"]["bucket"]["name"],
        event["Records"][0]["s3"]["object"]["key"],
    )

    zip_stream = io.BytesIO(compressed_insights.get()["Body"].read())

    with zipfile.ZipFile(zip_stream, "r") as zip_ref:
        insights = json.loads(zip_ref.read("insights.json").decode("utf-8"))

        image_data = Image.open(io.BytesIO(zip_ref.read("chart.png")))

    insights["examination_name"] = os.environ["EXAMINATION_NAME"]
    insights["examination_code"] = os.environ.get("EXAMINATION_CODE", None)
    insights["current_date"] = datetime.now().strftime("%d-%m-%Y")
    insights["emailer_name"] = os.environ["EMAILER_NAME"]

    gmail.send(
        subject=f"Performance Insights - {insights['examination_name']}{' - ' + insights['examination_code'] if insights['examination_code'] else ''} - {insights['current_date']}",
        receivers=[os.environ["EMAIL_RECIPIENT_EMAIL_ADDRESS"]],
        html_template="template.html",
        body_params={
            "data": insights
        },
        attachments={
            "chart.png": image_data
        }
    )

    return
