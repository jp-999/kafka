import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // For this stage, we'll just respond with a hard-coded correlation ID of 7
    const correlationId = 7;
    
    // Create the response header (4 bytes for correlation ID)
    const header = Buffer.alloc(4);
    header.writeInt32BE(correlationId, 0);
    
    // For this stage, we'll just send the header
    // The message_size will be 4 bytes (just the header size)
    const messageSize = Buffer.alloc(4);
    messageSize.writeInt32BE(4, 0);
    
    // Combine message_size and header
    const response = Buffer.concat([messageSize, header]);
    
    // Send the response
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1");
