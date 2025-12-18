FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install torch==2.2.0+cpu -f https://download.pytorch.org/whl/cpu/torch_stable.html
RUN pip install -r requirements.txt
CMD ["uvicorn", "ml_api:app", "--host", "0.0.0.0", "--port", "8002"]