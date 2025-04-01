
const PROCESSING_DELAY = 10000; // 5 seconds delay after 2 messages
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

process.on("message", async ({ fileName, records }) => {
    try {
        console.log(`🔹 Processing File: ${fileName} with ${records.length} records...`);

        for (const record of records) {
            console.log(`printing with dealy of 5 sec from  processID : [${process.pid} , ${fileName}] `, record);
            await delay(2000); // Ensures processing delay per record
        }

        // console.table(records);
        process.send({ success: true, fileName });
    } catch (error) {
        await delay(PROCESSING_DELAY);
        process.send({ success: false, fileName, error: error.message || error.toString() });
        process.exit(1);
    } finally {
        process.exit(0);
    }
});



async function insertToDatabase(records) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            if (Math.random() > 0.1) {
                resolve(); // Simulating successful DB insert
            } else {
                reject(new Error("Database insert failed")); // Simulating failure
            }
        }, 1000);
    });
}
async function isEven(records) {
    return new Promise((resolve, reject) => {
        if (records.length % 2 == 0) {
            resolve(); // Simulating successful DB insert
        } else {
            reject(new Error(`Odd detected  ${[records.length]}  records.`)); // Simulating failure
        }
    });
}


module.exports = { insertToDatabase };
