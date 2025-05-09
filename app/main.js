import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Constants
const API_VERSIONS_KEY = 18;  // ApiVersions API key
const UNSUPPORTED_VERSION = 35;  // Error code for unsupported version
const SUCCESS = 0;  // Success code
const MAX_SUPPORTED_VERSION = 4;  // Maximum supported version for ApiVersions

const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // Extract API key and version from the request
    const apiKey = data.readInt16BE(4);
    const apiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);
    
    // Create response buffers
    const messageSize = Buffer.alloc(4);
    const header = Buffer.alloc(4);
    let body;
    
    // Set correlation ID in header
    header.writeInt32BE(correlationId, 0);
    
    if (apiKey === API_VERSIONS_KEY) {
      // Handle ApiVersions request
      body = Buffer.alloc(2); // 2 bytes for error_code
      
      // Check if version is supported
      if (apiVersion > MAX_SUPPORTED_VERSION) {
        body.writeInt16BE(UNSUPPORTED_VERSION, 0);
      } else {
        body.writeInt16BE(SUCCESS, 0);
      }
      
      // Set message size (header size + body size)
      messageSize.writeInt32BE(4 + body.length, 0);
    } else {
      // For non-ApiVersions requests, maintain existing behavior
      messageSize.writeInt32BE(4, 0); // Just header size
      body = Buffer.alloc(0);
    }
    
    // Combine all parts and send response
    const response = Buffer.concat([messageSize, header, body]);
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1");
