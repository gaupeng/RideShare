import collections
import docker
import json
import os
import pika
import requests
import uuid

from datetime import datetime
from flask import jsonify, request
from model import doInit, Base, Ride, User
from requests.models import Response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from kazoo.client import KazooClient

# ------------------------------------------------------------------------------------
# Connect to the ZooKeeper container

zk = KazooClient(hosts='zoo:2181')
zk.start()

# Get environment variables specific to worker
workerType = os.environ['TYPE']
dbName = os.environ['DBNAME']
workerStatus = os.environ['CREATED']

# Initialise db connection
dbURI = doInit(dbName)
engine = create_engine(dbURI)
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)
session = Session()

# Connect to RMQ container
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rmq'))
channel = connection.channel()

# ------------------------------------------------------------------------------------


def slaveWatch(event):
    '''
    Function triggered on `event` that causes slave change.
    '''
    global connection, channel
    print("--> In slaveWatch <--")
    data, stat = zk.get("/root/"+name, watch=slaveWatch)
    data = data.decode("utf-8")
    channel.basic_cancel(consumer_tag="read")
    if data == "master":
        print("In Master!")

        # Initialise proper master connections
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq'))
        channel = connection.channel()
        channel.exchange_declare(exchange='syncQ', exchange_type='fanout')
        channel.queue_declare(queue='writeQ', durable=True)
        channel.basic_consume(
            queue='writeQ', on_message_callback=writeWrapMaster)
        channel.start_consuming()


def getSlavesCount():
    '''
    Get count on number of slaves.
    '''
    print("--> In getSlavesCount <--")
    pass_url = "http://worker_orchestrator_1:80/api/v1/zoo/count"
    r = requests.get(url=pass_url)
    resp = r.text
    resp = json.loads(resp)
    return resp


if workerType == "master":
    zk.create('/root/master', b'master', ephemeral=True)
else:
    name = str(getSlavesCount())
    print("Z-Node is:", name)
    zk.create('/root'+'/'+name, b'slave', ephemeral=True)
    data, stat = zk.get("/root/"+name, watch=slaveWatch)
    data = data.decode("utf-8")


# ------------------------------------------------------------------------------------

# Master Functionality

def checkHash(password):
    '''
    Check if `password` is 40 character hexadecimal.
    Return True if satisfied, and False otherwise.
    '''
    if len(password) == 40:
        password = password.lower()
        charSet = {"a", "b", "c", "d", "e", "f"}
        numSet = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}
        for char in password:
            if char not in charSet and char not in numSet:
                return False
        return True
    return False


def writeDB(req):
    '''
    Write request `req` to database.
    '''
    print("--> In writeDB <--")

    # get request data
    data = json.loads(req)

    # clear databases
    if data["table"] == "both":
        responseToReturn = Response()
        if len(session.query(User).all()) or len(session.query(Ride).all()):
            session.query(User).delete()
            session.query(Ride).delete()
            session.commit()
            responseToReturn.status_code = 200
        else:
            responseToReturn.status_code = 400
        return (responseToReturn.text, responseToReturn.status_code)

    elif data["table"] == "User":
        # Add a new User
        if data["caller"] == "addUser":
            responseToReturn = Response()
            if checkHash(data["password"]):
                newUser = User(
                    username=data["username"], password=data["password"])
                session.add(newUser)
                session.commit()
                responseToReturn.status_code = 201
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code)
        # Remove an existing User
        elif data["caller"] == "removeUser":
            session.query(User).filter_by(username=data["username"]).delete()
            session.query(Ride).filter_by(created_by=data["username"]).delete()
            session.commit()
            responseToReturn = Response()
            responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code)

    elif data["table"] == "Ride":
        # Add a new Ride
        if data["caller"] == "createRide":
            source = int(data["source"])
            dest = int(data["destination"])
            responseToReturn = Response()
            noRows = 198
            if source in range(1, noRows + 1) and dest in range(1, noRows + 1):
                newRide = Ride(created_by=data["created_by"], username="", timestamp=data["timestamp"],
                               source=source, destination=dest)
                session.add(newRide)
                session.commit()
                responseToReturn.status_code = 201
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code)

        # join ride
        elif data["caller"] == "joinRide":
            rideExists = session.query(Ride).filter_by(
                ride_id=data["rideId"]).first()
            if rideExists.username:
                rideExists.username += ", " + data["username"]
            else:
                rideExists.username += data["username"]
            session.commit()
            responseToReturn = Response()
            responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code)

        # delete ride
        elif data["caller"] == "deleteRide":
            session.query(Ride).filter_by(ride_id=data["rideId"]).delete()
            session.commit()
            responseToReturn = Response()
            responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code)


# -----------------------------------------------------------------------------------

# Master Code

# Wrapper for writeDB
def writeWrapMaster(ch, method, props, body):
    '''
    Called on consuming from writeQ.
    '''
    body = json.dumps(eval(body.decode()))
    writeDB(body)
    channel.basic_publish(exchange='syncQ', routing_key='', body=body, properties=pika.BasicProperties(
        reply_to=props.reply_to,
        correlation_id=props.correlation_id,
        delivery_mode=2))
    # Ideally we want to comment out below and get respsonse at orch from slave
    # ch.basic_publish(exchange='', routing_key=props.reply_to, properties=pika.BasicProperties(
    #     correlation_id=props.correlation_id), body=str(writeResponse))
    # ch.basic_ack(delivery_tag=method.delivery_tag)


# Consume from writeQ for Master
if workerType == 'master':
    print("In Master!")
    zk.ensure_path('/master/node')
    channel.exchange_declare(exchange='syncQ', exchange_type='fanout')
    channel.queue_declare(queue='writeQ', durable=True)
    channel.basic_consume(queue='writeQ', on_message_callback=writeWrapMaster)
    channel.start_consuming()

# -----------------------------------------------------------------------------------

# Slave Code


def writeWrapSlave(ch, method, props, body):
    print("--> In writeWrapSlave <--")
    body = json.dumps(eval(body.decode()))
    writeResponse = writeDB(body)
    channel.basic_publish(exchange='', routing_key='responseQ', properties=pika.BasicProperties(
        correlation_id=props.correlation_id,  delivery_mode=2), body=str(writeResponse))


def timeAhead(timestamp):
    '''
    Ensure time `timestamp` is ahead of current time.
    Return True if so and Falsew otherwise.
    '''
    currTimeStamp = datetime.now().isoformat(' ', 'seconds')
    currTimeStamp = datetime.strptime(currTimeStamp, "%Y-%m-%d %H:%M:%S")
    convertedTimeStamp = datetime.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")
    if currTimeStamp < convertedTimeStamp:
        return True
    return False


def readDB(req):
    '''
    Read request `req` from database.
    '''
    # get request data
    data = json.loads(req)

    if data["table"] == "User":
        # check user exists
        checkUserSet = {"removeUser", "createRide"}
        if data["caller"] in checkUserSet:
            userExists = session.query(User).filter_by(
                username=data["username"]).all()
            responseToReturn = Response()
            if userExists:
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code)

        elif data["caller"] == "addUser":
            # check user exists
            userExists = session.query(User).filter_by(
                username=data["username"]).all()
            responseToReturn = Response()
            if userExists:
                responseToReturn.status_code = 400
            else:
                responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code)

        elif data["caller"] == "checkUser":
            # check user exists
            userExists = session.query(User).all()
            userList = list()
            for users in userExists:
                userList.append(users.as_dict())
            responseToReturn = Response()
            if len(userList):
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (json.dumps(userList), responseToReturn.status_code)

    elif data["table"] == "Ride":
        if data["caller"] == "listUpcomingRides":
            # list upcoming rides
            rides = session.query(Ride).all()
            returnObj = []
            for ride in rides:
                if ride.source == data["source"] and ride.destination == data["destination"]:
                    if timeAhead(ride.timestamp):
                        newObj = {
                            "rideId": ride.ride_id, "username": ride.created_by, "timestamp": ride.timestamp}
                        returnObj.append(newObj)
            responseToReturn = Response()
            print(returnObj)
            if not returnObj:
                responseToReturn.status_code = 204
            else:
                responseToReturn.status_code = 200
            return (json.dumps(returnObj), responseToReturn.status_code)

        elif data["caller"] == "countRides":
            rides = session.query(Ride).all()
            return (json.dumps(len(rides)), 200)

        elif data["caller"] == "listRideDetails":
            # list ride details
            rides = session.query(Ride).all()
            userArray = []
            dictToReturn = dict()
            responseToReturn = Response()
            rideNotFound = True
            for ride in rides:
                if ride.ride_id == int(data["rideId"]):
                    rideNotFound = False
                    userArray = ride.username.split(", ")
                    if userArray[0] == "":
                        userArray.clear()
                    responseToReturn.status_code = 200
                    keyValues = [("rideId", ride.ride_id), ("created_by", ride.created_by),
                                 ("users", userArray), ("timestamp",
                                                        ride.timestamp), ("source", ride.source),
                                 ("destination", ride.destination)]
                    dictToReturn = collections.OrderedDict(keyValues)
                    break
            if rideNotFound:
                responseToReturn.status_code = 204
            return (json.dumps(dictToReturn), responseToReturn.status_code)

        elif data["caller"] == "deleteRide":
            # check ride exists
            rideExists = session.query(Ride).filter_by(
                ride_id=data["rideId"]).all()
            responseToReturn = Response()
            if rideExists:
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code)

        elif data["caller"] == "joinRide":
            # check if user and ride exist
            userExists = session.query(User).filter_by(
                username=data["username"]).all()
            rideExists = session.query(Ride).filter_by(
                ride_id=data["rideId"]).all()
            responseToReturn = Response()
            if userExists and rideExists:
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code)


# Wrapper for read
def readWrap(ch, method, props, body):
    print("--> In readWrap <--")
    body = json.dumps(eval(body.decode()))
    readResponse = readDB(body)
    channel.basic_publish(exchange='', routing_key='responseQ', properties=pika.BasicProperties(
        correlation_id=props.correlation_id), body=str(readResponse))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def actualSync(users_rides):
    '''
    Sync functionality. `users_rides` is [users, rides]
    '''
    global workerStatus
    workerStatus = "OLD"
    print("Users:\n")
    for users in users_rides[1]:
        print(users["username"], users["password"])
        newUser = User(username=users["username"], password=users["password"])
        session.add(newUser)
        session.commit()
    print("Rides:\n")
    for rides in users_rides[0]:
        print(rides["created_by"], rides["username"],
              rides["timestamp"], rides["source"], rides["destination"])
        newRide = Ride(created_by=rides["created_by"], username=rides["username"],
                       timestamp=rides["timestamp"], source=rides["source"], destination=rides["destination"])
        session.add(newRide)
        session.commit()


def syncDB(mdbName):
    print("--> In syncDB <--")
    pass_url = "http://worker_orchestrator_1:80/api/v1/db/sync"
    r = requests.get(url=pass_url)
    resp = r.text
    resp = json.loads(resp)
    actualSync(resp)


# Consume from readQ for Slave
if workerType == 'slave':
    print("In Slave!", workerStatus)
    if(workerStatus == "NEW"):
        print("I AM NEW")
        syncDB("postgres_worker")
    print(workerStatus)

    channel.queue_declare(queue='readQ', durable=True)
    channel.queue_declare(queue='responseQ', durable=True)
    channel.basic_qos(prefetch_count=1)

    # Sync database with master
    channel.exchange_declare(exchange='syncQ', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True, durable='True')
    queue_name = result.method.queue
    channel.basic_consume(
        queue=queue_name, on_message_callback=writeWrapSlave, auto_ack=True)
    channel.queue_bind(exchange='syncQ', queue=queue_name)

    # Read after sync
    channel.basic_consume(
        queue='readQ', on_message_callback=readWrap, consumer_tag="read")
    channel.start_consuming()
# -----------------------------------------------------------------------------------
