import sys
sys.path.append('/home/ubuntu/rideshareUserMicroService')
sys.path.append('/rideshareUserMicroService')
import json, requests, collections
from datetime import datetime
from flask import Flask, request, jsonify
from model import db, Ride, User
import requests
from requests.models import Response
import pandas as pd


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://ubuntu:ride@user_db:5454/postgres'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
with app.app_context():
    db.create_all()

df = pd.read_csv("AreaNameEnum.csv")
noRows = df.shape[0]

callCount = 0

def updateCount():
    global callCount
    callCount += 1
    return callCount

def checkUser(username):
    requrl = 'http://loadbalancer-567847655.us-east-1.elb.amazonaws.com/api/v1/users'
    userList = requests.get(requrl, headers = {"Origin": "ec2-54-84-10-21.compute-1.amazonaws.com"}).content.decode()
    if username in userList:
        return True	
    return False


# 1: Add User
@app.route('/api/v1/users', methods = ["GET", "POST", "PUT", "DELETE"])
def addUser():
    '''
    Add User based on data from PUT request.
    '''
    updateCount()
    result = dict()
    data = request.get_json()
    # Add User
    if request.method == "PUT":
        rurl = "http://34.237.33.12:80/api/v1/db/read"
        readObjToPass = {"username": data["username"], "password": data["password"], "table":"User", "caller":"addUser"}
        res = requests.post(rurl, json = readObjToPass)
        if res.status_code == 200:
            wurl = "http://34.237.33.12:80/api/v1/db/write"
            writeObjToPass = {"username": data["username"], "password": data["password"], "table":"User", "caller":"addUser"}
            resp = requests.post(wurl, json = writeObjToPass)
            return jsonify(result), resp.status_code
        return jsonify(result), 400

    # List Users
    elif request.method == "GET":
        rurl = "http://34.237.33.12:80/api/v1/db/read"
        readObj = {"table":"User","caller":"checkUser"}
        res = requests.post(rurl, json=readObj)
        res = json.loads(res.text)
        UserRows = res
        print(UserRows)
        users = []
        for userRow in UserRows:
            users.append(userRow["username"])
        if(len(users)):
            return jsonify(users), 200
        else:
            return jsonify(users), 204

    else:
        return jsonify(result), 405



# 2: Remove User
@app.route('/api/v1/users/<username>', methods = ["GET", "POST", "PUT", "DELETE"])
def removeUser(username):
    '''
    Remove a user with specified `username`.
    '''
    updateCount()
    result = dict()
    if request.method == 'DELETE':
        rurl = "http://34.237.33.12:80/api/v1/db/read"
        readObjToPass = {"username": username, "table":"User", "caller":"removeUser"}
        res = requests.post(rurl, json = readObjToPass)
        if res.status_code == 200:
            wurl = "http://34.237.33.12:80/api/v1/db/write"
            writeObjToPass = {"username": username, "table":"User", "caller":"removeUser"}
            resp = requests.post(wurl, json = writeObjToPass)
            return jsonify(result), resp.status_code
        else:
            return jsonify(result), 400
    else:
        return jsonify(result), 405


# 8: Write to Database
@app.route('/api/v1/db/write', methods = ["POST"])
def writetoDB():
    data = request.get_json()
    if data["table"] == "User":
        # Add a new User
        if data["caller"] == "addUser":
            responseToReturn = Response()
            if checkHash(data["password"]):
                newUser = User(username = data["username"], password = data["password"])
                db.session.add(newUser)
                db.session.commit()
                responseToReturn.status_code = 201
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())
        # Remove an existing User     
        elif data["caller"] == "removeUser":
            User.query.filter_by(username = data["username"]).delete()
            Ride.query.filter_by(created_by = data["username"]).delete()
            db.session.commit()
            responseToReturn = Response()
            responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())

    elif data["table"] == "Ride":
        # Add a new Ride
        if data["caller"] == "createRide":
            source = int(data["source"])
            dest = int(data["destination"])
            responseToReturn = Response()
            if source in range(1, noRows + 1) and dest in range(1, noRows + 1):
                newRide = Ride(created_by = data["created_by"], username = "", timestamp = data["timestamp"],
                        source = source, destination = dest)
                db.session.add(newRide)
                db.session.commit()
                responseToReturn.status_code = 201
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())

        elif data["caller"] == "joinRide":
            rideExists = Ride.query.filter_by(ride_id = data["rideId"]).first()
            if rideExists.username:
                rideExists.username += ", " + data["username"]
            else:
                rideExists.username += data["username"]
            db.session.commit()
            responseToReturn = Response()
            responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())

        elif data["caller"] == "deleteRide":
            Ride.query.filter_by(ride_id = data["rideId"]).delete()
            db.session.commit()
            responseToReturn = Response()
            responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())


# 9: Read from database
@app.route('/api/v1/db/read', methods = ["POST"])
def readfromDB():
    data = request.get_json()
    if data["table"] == "User":
        checkUserSet = {"removeUser", "createRide"}
        if data["caller"] in checkUserSet:
            userExists = User.query.filter_by(username = data["username"]).all()
            responseToReturn = Response()
            if userExists:
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())
        
        elif data["caller"] == "addUser":
            userExists = User.query.filter_by(username = data["username"]).all()
            responseToReturn = Response()
            if userExists:
                responseToReturn.status_code = 400
            else:
                responseToReturn.status_code = 200
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())
        
    elif data["table"] == "Ride":
        if data["caller"] == "listUpcomingRides":
            rides = Ride.query.all()
            returnObj = []
            for ride in rides:
                if ride.source == data["source"] and ride.destination == data["destination"]:
                    if timeAhead(ride.timestamp):
                        newObj = {"rideId":ride.ride_id, "username":ride.created_by, "timestamp":ride.timestamp}
                        returnObj.append(newObj)
            responseToReturn = Response()
            if not returnObj:
                responseToReturn.status_code = 204
            else:
                responseToReturn.status_code = 200
            return (jsonify(returnObj), responseToReturn.status_code, responseToReturn.headers.items())
        
        elif data["caller"] == "listRideDetails":
            rides = Ride.query.all()
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
                    ("users", userArray), ("timestamp", ride.timestamp), ("source", ride.source),
                    ("destination", ride.destination)]
                    dictToReturn = collections.OrderedDict(keyValues)
                    break
            if rideNotFound:
                responseToReturn.status_code = 204
            return (jsonify(dictToReturn), responseToReturn.status_code, responseToReturn.headers.items())

        elif data["caller"] == "deleteRide":
            rideExists = Ride.query.filter_by(ride_id = data["rideId"]).all()
            responseToReturn = Response()
            if rideExists:
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())

        elif data["caller"] == "joinRide":
            userExists = User.query.filter_by(username = data["username"]).all()
            rideExists = Ride.query.filter_by(ride_id = data["rideId"]).all()
            responseToReturn = Response()
            if userExists and rideExists:
                responseToReturn.status_code = 200
            else:
                responseToReturn.status_code = 400
            return (responseToReturn.text, responseToReturn.status_code, responseToReturn.headers.items())


def checkHash(password):
    if len(password) == 40:
        password = password.lower()
        charSet = {"a", "b", "c", "d", "e", "f"}
        numSet = {'1','2','3','4','5','6','7','8','9','0'}
        for char in password:
            if char not in charSet and char not in numSet:
                return False
        return True
    return False


def timeAhead(timestamp):
    currTimeStamp = datetime.now().isoformat(' ', 'seconds')
    currTimeStamp = datetime.strptime(currTimeStamp, "%Y-%m-%d %H:%M:%S")
    convertedTimeStamp = datetime.strptime(timestamp, "%d-%m-%Y:%S-%M-%H")
    if currTimeStamp < convertedTimeStamp:
        return True
    return False

# Assignment 2
            
@app.route('/api/v1/db/clear', methods = ["POST"])
def clearData():
    result = dict()
    if request.method == "POST":
        if(len(User.query.all())):
            User.query.delete()
            db.session.commit()
            return jsonify(result), 200
        else:
            return jsonify(result), 400

    else:
        return jsonify(result), 405


# Assignment 3
@app.route('/api/v1/_count', methods = ["GET"])
def countCalls():
    result = []
    if request.method == "GET":
        result.append(callCount)
        return jsonify(result), 200
    else:
        return jsonify(result), 405

@app.route('/api/v1/_count', methods = ["DELETE"])
def clearCallCount():
    result = dict()
    if request.method == "DELETE":
        global callCount
        callCount = 0
        return jsonify(result), 200
    return jsonify(result), 405


if __name__ == '__main__':
    app.debug = True
    app.run(host = '0.0.0.0',port='80')
