const fs = require("fs");
const path = require("path");
const { connectRabbitMQ } = require("./rabbitMQ/rabbitmq");
const { fork } = require("child_process");

const QUEUE_NAME = "csv_queue";
const DLQ_NAME = "csv_failed_queue";
const PREFETCH = 3;
let channel;
let connection;


// Function to write logs to JSON file
function writeLogToFile(logData) {
    // Define log file path
    const LOG_FILE = path.join(__dirname, "logs.json");
    const logFileExists = fs.existsSync(LOG_FILE);
    let logs = [];
    
    if (logFileExists) {
        try {
            logs = JSON.parse(fs.readFileSync(LOG_FILE, "utf8"));
        } catch (error) {
            console.error("Error parsing existing logs:", error);
        }
    }
    
    logs.push(logData); // Append new log entry
    
    fs.writeFileSync(LOG_FILE, JSON.stringify(logs)); // Save updated logs
}


const PROCESSING_DELAY = 10000; // 5 seconds delay after 2 messages
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Consumer function
async function startConsumer(queueName) {
    if (!channel) {
        const conn = await connectRabbitMQ();
        connection = conn.connection;
        channel = conn.channel;

        await channel.assertQueue(QUEUE_NAME, { durable: true });
        await channel.assertQueue(DLQ_NAME, { durable: true });

        channel.prefetch(PREFETCH);
    }

    console.log(`👷 Worker [PID: ${process.pid}] listening on queue: ${queueName} with prefetch: ${PREFETCH}...`);

    channel.consume(queueName, async (msg) => {
        if (msg !== null) {
            try {
                let { fileName, records } = JSON.parse(msg.content.toString());

                // Logging the file processing in JSON
                writeLogToFile({ timestamp: new Date().toISOString(), event: "Processing File", fileName, pid: process.pid, status: "Started", });

                console.log(`📥 Received File: ${fileName}, Records: ${records.length}`);

                let child = fork("./worker.js");
                child.send({ fileName, records  });

                child.on("message", async(status) => {
                    if (status.error) {
                        console.error(`  Error processing File: ${fileName}, Error: ${status.error} \n`);
                        writeLogToFile({ timestamp: new Date().toISOString(), event: "Error Processing File", fileName, pid: process.pid, status: "Failed", error: status.error, });
                        // channel.sendToQueue(DLQ_NAME, msg.content, { persistent: true });
                        // await delay(PROCESSING_DELAY);
                        channel.ack(msg);
                        return;
                    }
                    console.log(`✅ File Processed: ${fileName} with child PID: [${child.pid}] and acknowledged message \n`);
                    writeLogToFile({ timestamp: new Date().toISOString(), event: "File Processed", fileName, pid: child.pid, status: "Success", });
                    
                    // delay after 10 seconds before acknowledging the message
                    // await delay(PROCESSING_DELAY);
                    channel.ack(msg);
                });

                child.on("exit",async (status) => {
                    // Process exited, no extra logs needed

                    //dealy 
                    // await delay(PROCESSING_DELAY);
                    console.log('✅ Process exited with code 0 ----');
                   
                });

            } catch (error) {
                console.error(`❌ Error processing File: ${fileName}, Moving to DLQ`);
                writeLogToFile({ timestamp: new Date().toISOString(), event: "Moving to DLQ", fileName: msg?.content?.toString() || "Unknown", pid: process.pid, status: "DLQ", error: error.message, });
                // channel.sendToQueue(DLQ_NAME, msg.content, { persistent: true });
                channel.ack(msg);
            }
        }
    });
}

// Handle PM2 & graceful shutdown
process.on("SIGINT", async () => {
    console.log("🔴 Closing RabbitMQ connection...");
    writeLogToFile({timestamp: new Date().toISOString(),event: "Shutdown",pid: process.pid,status: "Closing RabbitMQ",});
    await channel.close();
    await connection.close();
    process.exit(0);
});

// Start consumer
startConsumer(QUEUE_NAME);
