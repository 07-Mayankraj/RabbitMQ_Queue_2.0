require("dotenv").config();
const amqp = require("amqplib");

const RABBITMQ_CONFIG = {
    protocol: "amqp",
    hostname: process.env.RABBITMQ_HOST,
    port: process.env.RABBITMQ_PORT || 5672,
    username: process.env.RABBITMQ_USER,
    password: process.env.RABBITMQ_PASS,
    vhost: process.env.RABBITMQ_VHOST || "/",
    heartbeat: 60,
};

let channel = null;
let connection = null;
async function connectRabbitMQ() {
    try {
        console.log("🔄 Connecting to RabbitMQ...");
        connection = await amqp.connect(RABBITMQ_CONFIG);
        channel = await connection.createChannel();
        console.log("✅ RabbitMQ Connected Securely!");
        return { connection, channel };
    } catch (error) {
        console.error("❌ RabbitMQ Connection Error:", error);
        setTimeout(connectRabbitMQ, 5000);
    }
}

async function closeRabbitMQ() {
    if (channel) await channel.close();
    if (connection) await connection.close();
}


module.exports = { connectRabbitMQ,closeRabbitMQ };
