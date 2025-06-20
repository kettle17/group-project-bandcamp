FROM public.ecr.aws/lambda/python:latest

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY alerts.py .

CMD ["alerts.alerts_lambda_handler"]