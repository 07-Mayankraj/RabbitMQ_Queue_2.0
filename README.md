# RabbitMQ_Queue_
A node js application that implements the RabbitMQ  Queue interface

run consumer using pm2 `node consumer.js `

Consumers : Consumer will connect to RabbitMQ and fetch messages and logs on console continuously.
Producer : Whenever Producer get invoked, it will read csv file and send it to RabbitMQ queue 

invoked both in separate terminals

