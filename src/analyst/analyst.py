import json
import pandas
from datetime import datetime
from dotenv import load_dotenv
import os
import boto3

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


def store_insights(insights: dict):
    s3 = boto3.resource("s3")

    bucket = s3.Bucket(os.environ["S3_BUCKET_NAME"])

    bucket.put_object(
        Key=os.path.join(
            os.environ["S3_BUCKET_FOLDER"],
            f"results-{datetime.now().strftime('%d-%m-%Y')}",
        ),
        Body=json.dumps(insights),
    )


def handler(event, context):
    insights = get_insights()

    dump_results = os.environ.get("DUMP_RESULTS", None)

    if not dump_results or dump_results == "False":
        return insights

    store_insights(insights)

    return insights
