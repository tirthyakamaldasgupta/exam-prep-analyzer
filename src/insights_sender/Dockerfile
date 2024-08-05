FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY insights_sender.py ${LAMBDA_TASK_ROOT}

CMD [ "insights_sender.handler" ]