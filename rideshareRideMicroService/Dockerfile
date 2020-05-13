FROM python
ADD . /rideshareRideMicroService
WORKDIR /rideshareRideMicroService
ENV FLASK_APP __init__.py
RUN pip install -r requirements.txt
EXPOSE 80
CMD ["python3","__init__.py"]
