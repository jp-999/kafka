import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Constants
const API_VERSIONS_KEY = 18;  // ApiVersions API key
const UNSUPPORTED_VERSION = 35;  // Error code for unsupported version
const SUCCESS = 0;  // Success code

const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // Extract request details
    const apiKey = data.readInt16BE(4);
    const apiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);
    
    console.log(`Received request: apiKey=${apiKey}, apiVersion=${apiVersion}, correlationId=${correlationId}`);
    
    let response;
    
    if (apiKey === API_VERSIONS_KEY) {
      // Handle ApiVersions request
      console.log("Processing ApiVersions request");
      
      // Create a fixed response for ApiVersions v4
      response = Buffer.alloc(23);
      let offset = 0;
      
      // Message size (4 bytes) - 19 bytes for the rest of the message
      response.writeInt32BE(19, offset);
      offset += 4;
      
      // Correlation ID (4 bytes) - from the request
      response.writeInt32BE(correlationId, offset);
      offset += 4;
      
      // Error code (2 bytes) - 0 = SUCCESS
      response.writeInt16BE(SUCCESS, offset);
      offset += 2;
      
      // API keys array length (1 byte) - 2 in COMPACT_ARRAY format (N+1)
      response.writeUInt8(2, offset);
      offset += 1;
      
      // API key entry (6 bytes)
      response.writeInt16BE(API_VERSIONS_KEY, offset);  // API key (18 = API_VERSIONS)
      offset += 2;
      response.writeInt16BE(0, offset);   // Min version (0)
      offset += 2;
      response.writeInt16BE(4, offset);   // Max version (4)
      offset += 2;
      
      // API key entry tag buffer (1 byte) - 0 = no tagged fields
      response.writeUInt8(0, offset);
      offset += 1;
      
      // Throttle time (4 bytes) - 0
      response.writeInt32BE(0, offset);
      offset += 4;
      
      // Response tag buffer (1 byte) - 0 = no tagged fields
      response.writeUInt8(0, offset);
    } else {
      // For non-ApiVersions requests, just return a header with correlation ID
      console.log("Processing non-ApiVersions request");
      
      response = Buffer.alloc(8);
      
      // Message size (4 bytes) - 4 bytes for the header
      response.writeInt32BE(4, 0);
      
      // Correlation ID (4 bytes) - from the request
      response.writeInt32BE(correlationId, 4);
    }
    
    // Send the response
    console.log(`Sending response of ${response.length} bytes`);
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1", () => {
  console.log("Kafka server listening on port 9092");
});
