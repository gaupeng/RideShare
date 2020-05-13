# RideShare
Backend for a cloud based application **RideShare**, that can be used to pool people on rides. This project was done as a part of our University's course project under the subject of Cloud Computing.

* This application is built using Flask, and was deployed on an EC2 Instance, through Amazon Web Services.
* Features covered by codebase include: <br>
    1. Database as a Service (DBaaS).
    2. High availability for both master, and slaves.
    3. Dynamic scalability, by scaling up/down workers based on number of requests.
* Technology Used:
    1. **Python** - Almost all of the code (except some command line scripts for ease of running), used Python.
    2. **Flask** - Our application ran on Flask. This included the rides microservice, the users microservice and the orchestrator.
    3. **PostgreSQL** - Our primary database used was PostgreSQL. Sometimes, we used files to keep track of counts persistently.
    4. **RabbitMQ** - RabbitMQ acted as our message-broker through the different components of the application.
    5. **Apache ZooKeeper** - ZooKeeper was used to create and maintain working entities in the form of nodes.
    * Side note: We also had a load balancer that routed requests based on rules defined by us.
* Application Flow:
    1. We had three instances running: <br>
        a. The Users micro-service. <br>
        b. The Rides micro-service. <br>
        c. The Orchestrator.
    2. Any incoming request, would reach the Application Load Balancer, where based on target group rules, would be redirected to either the Users or Rides micro-service.
    3. Upon requiring the usage of database on one of these micro-services, the request would be forwarded to the Orchestrator from these micro-services.
    4. The orchestrator maintained workers, in the form of a single master worker, and multiple slave workers. Each worker maintains its own copy of the database.
    5. On receiving a write request, the orchestrator would write to a RabbitMQ queue, called the 'writeQ'.
    6. The master worker is responsible for all writes to the database, and hence has to ensure that the slaves keep their database in sync with itself. After writing to its database, the master would write to a RabbitMQ exchange, to notify all slaves to update their database.
    7. The slaves on receiving this message to sync their database with the master, would update their copy of the database with the new values. 
    8. Any read requests to the orchestrator would be processed by the slave workers in a round-robin fashion, after the orchestrator publishes the message to a RabbitMQ queue 'readQ'.
    9. Thus, any response, whether write or read, would at the end, be acknowledged at the orchestrator, when a slave confirms its actions.
* Running The Code:
    * To run the code on any of the intances, simply do:
    ```docker-compose build && docker-compose up```.
    * Requests were tested on Postman.

* Team:
    Our team consisted of:
    1. Anantharam R U
    2. Gaurang Rao
    3. Shashank Prasad
    4. Tanay Gangey