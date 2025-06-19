FROM public.ecr.aws/lambda/python:latest

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY tracktion_logo_transparent_light.png .
COPY queries.py .
COPY pdf_class.py .
COPY report.py .


CMD ["report.report_lambda_handler"]
