import io
import json
import pandas
from datetime import datetime
from dotenv import load_dotenv
import os
import boto3
from matplotlib import pyplot
from matplotlib.patches import Patch

load_dotenv()


def get_correct_answers_insights(
    dataframe: pandas.DataFrame, number_of_attempted_questions: int
) -> dict:
    insights = {}

    insights["number"] = len(
        dataframe.loc[dataframe["Correctness"] == True, "Correctness"]
    )

    if insights["number"] < 1:
        insights["percentage"] = None

        return insights

    insights["percentage"] = round(
        (insights["number"] / number_of_attempted_questions) * 100, 2
    )

    return insights


def get_incorrect_answers_insights(
    dataframe: pandas.DataFrame, number_of_attempted_questions: int
) -> dict:
    insights = {}

    insights["number"] = len(
        dataframe.loc[dataframe["Correctness"] == False, "Correctness"]
    )

    if insights["number"] < 1:
        insights["percentage"] = None

        return insights

    insights["percentage"] = round(
        (insights["number"] / number_of_attempted_questions) * 100,
        2,
    )

    incorrect_answers_detailed_insights = []

    causes = dataframe["Failure Reason"].unique()
    causes = [cause for cause in causes if type(cause) == str]

    if len(causes) == 0:
        insights["detailed_insights"] = None

        return insights

    for cause in causes:
        incorrect_answers_detailed_insights.append(
            {
                "cause": cause,
                "number": len(
                    dataframe.loc[
                        dataframe["Failure Reason"] == cause, "Failure Reason"
                    ]
                ),
                "percentage": round(
                    (
                        len(
                            dataframe.loc[
                                dataframe["Failure Reason"] == cause,
                                "Failure Reason",
                            ]
                        )
                        / insights["number"]
                    )
                    * 100,
                    2,
                ),
            }
        )

    insights["detailed_insights"] = incorrect_answers_detailed_insights

    return insights


def get_insights() -> dict:
    insights = {}

    dataframe = pandas.read_csv(os.environ["SPREADSHEET_URL"])

    dataframe = dataframe.loc[
        (
            dataframe["Date Attempted"]
            == datetime.now().strftime(os.environ["DATE_ATTEMPTED_COLUMN_VALUE_FORMAT"])
        )
        & (dataframe["Attempted"] == True)
    ]

    if dataframe.empty:
        insights["questions_attempted"] = 0
        insights["insights"] = None

        return insights

    insights["questions_attempted"] = len(dataframe)

    correct_answers_insights = get_correct_answers_insights(
        dataframe, insights["questions_attempted"]
    )

    incorrect_answers_insights = get_incorrect_answers_insights(
        dataframe, insights["questions_attempted"]
    )

    insights["insights"] = {}
    insights["insights"]["correct"] = correct_answers_insights
    insights["insights"]["incorrect"] = incorrect_answers_insights

    return insights


def get_chart_data(insights: dict) -> dict:
    labels = ["Correct Answers"]
    labels.extend(
        [
            incorrect_answers_detailed_insight["cause"]
            for incorrect_answers_detailed_insight in insights["insights"]["incorrect"][
                "detailed_insights"
            ]
        ]
    )

    sizes = [insights["insights"]["correct"]["number"]]
    sizes.extend(
        [
            incorrect_answers_detailed_insight["number"]
            for incorrect_answers_detailed_insight in insights["insights"]["incorrect"][
                "detailed_insights"
            ]
        ]
    )

    return {"labels": labels, "sizes": sizes}


def get_chart_buffer(chart_data):
    image_buffer = io.BytesIO()

    pyplot.figure(figsize=(10, 6))
    pyplot.pie(
        chart_data["sizes"],
        labels=chart_data["labels"],
        autopct="%1.1f%%",
        startangle=140,
    )
    pyplot.axis("equal")

    # Save the plot to the BytesIO buffer
    pyplot.savefig(image_buffer, format="png")

    # Seek to the start of the buffer
    image_buffer.seek(0)

    with open("1.png", 'wb') as f:
        f.write(image_buffer.getvalue())

    pyplot.close()

    return image_buffer


def store_insights(insights: dict, image):
    s3 = boto3.resource("s3")

    bucket = s3.Bucket(os.environ["S3_BUCKET_NAME"])

    bucket.put_object(
        Key=os.path.join(
            os.environ["S3_BUCKET_FOLDER"],
            datetime.now().strftime("%d-%m-%Y"),
            f"insights.json",
        ),
        Body=json.dumps(insights),
    )

    bucket.put_object(
        Key=os.path.join(
            os.environ["S3_BUCKET_FOLDER"],
            datetime.now().strftime("%d-%m-%Y"),
            f"chart.png",
        ),
        Body=image,
        ContentType="image/png",
    )


def handler(event, context):
    insights = get_insights()

    dump_results = os.environ.get("DUMP_RESULTS", None)

    if not dump_results or dump_results == "False":
        return insights

    chart_data = get_chart_data(insights)

    chart_buffer = get_chart_buffer(chart_data)

    store_insights(insights, chart_buffer)

    return insights
