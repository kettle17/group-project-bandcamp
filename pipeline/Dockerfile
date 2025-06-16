FROM public.ecr.aws/lambda/python:latest

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY etl_controller.py .
COPY extract.py .
COPY transform.py .
COPY load.py .
COPY utilities.py .
COPY web_scraper.py .

CMD ["etl_controller.etl_lambda_handler"]
