import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // Extract correlation ID from the request
    // Skip message_size (4 bytes) and request_api_key + request_api_version (4 bytes)
    // Then read the next 4 bytes as correlation ID
    const correlationId = data.readInt32BE(8);
    
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
