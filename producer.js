const { connectRabbitMQ } = require("./rabbitMQ/rabbitmq");
const fs = require("fs");
const csvParser = require("fast-csv");
const path = require("path");

async function sendCsvToQueue(filePath) {
    let { channel } = await connectRabbitMQ();
    const QUEUE_NAME = "csv_queue";

    await channel.assertQueue(QUEUE_NAME, { durable: true });

    let records = [];

    fs.createReadStream(filePath)
        .pipe(csvParser.parse({ headers: true }))
        .on("data", (row) => {
            row.fileName = path.basename(filePath);
            records.push(row);
        })
        .on("end", () => {
            if (records.length > 0) {
                channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify({ fileName : path.basename(filePath), records })), {
                    persistent: true,
                });
            }
            console.log(`✅ CSV file ${filePath} sent to ${QUEUE_NAME}.`);
            setTimeout(() => {
                console.log('closing...');
                process.exit(0);
            }, 5000);
        });
}

// Example Usage
// sendCsvToQueue("./data/userData.csv", "useData.csv");
sendCsvToQueue("./data/mayank.csv", "mayank.csv");
sendCsvToQueue("./data/xyz.csv", "xyz.csv");



fs.readdir("./data/", (err, files) => {
    if (err) {
        console.error("Error reading directory:", err);
        return;
    }
    files.forEach(file => {
        if (file.endsWith(".csv")) {
            sendCsvToQueue(path.join("./data/", file));
        }
    });
})