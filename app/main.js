import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Constants
const API_VERSIONS_KEY = 18;  // ApiVersions API key
const UNSUPPORTED_VERSION = 35;  // Error code for unsupported version
const SUCCESS = 0;  // Success code
const MAX_SUPPORTED_VERSION = 4;  // Maximum supported version for ApiVersions
const MIN_SUPPORTED_VERSION = 0;  // Minimum supported version for ApiVersions

// Define supported API keys and their version ranges
const supportedApis = [
  { apiKey: API_VERSIONS_KEY, minVersion: 0, maxVersion: 4 }
  // Add more supported APIs here as needed
];

const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // Extract API key and version from the request
    const apiKey = data.readInt16BE(4);
    const apiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);
    
    // Create response buffers
    let header = Buffer.alloc(4);
    let body;
    
    // Set correlation ID in header
    header.writeInt32BE(correlationId, 0);
    
    if (apiKey === API_VERSIONS_KEY) {
      // Handle ApiVersions request
      if (apiVersion < MIN_SUPPORTED_VERSION || apiVersion > MAX_SUPPORTED_VERSION) {
        // Unsupported version
        body = Buffer.alloc(2);
        body.writeInt16BE(UNSUPPORTED_VERSION, 0);
      } else {
        // Success response with proper APIVersions body structure
        
        // For APIVersions v3/v4, the response structure is:
        // error_code (int16) + api_key_responses array + throttle_time_ms (int32)
        
        // Start with error code (2 bytes)
        const errorCodeBuffer = Buffer.alloc(2);
        errorCodeBuffer.writeInt16BE(SUCCESS, 0);
        
        // Create buffer for API versions array
        const apiCount = supportedApis.length;
        const apiCountBuffer = Buffer.alloc(2);
        apiCountBuffer.writeInt16BE(apiCount, 0);
        
        // Create buffer for API entries
        const apiEntriesBuffer = Buffer.alloc(apiCount * 6); // 6 bytes per API entry
        let offset = 0;
        for (const api of supportedApis) {
          apiEntriesBuffer.writeInt16BE(api.apiKey, offset);
          apiEntriesBuffer.writeInt16BE(api.minVersion, offset + 2);
          apiEntriesBuffer.writeInt16BE(api.maxVersion, offset + 4);
          offset += 6;
        }
        
        // Add throttle_time_ms (int32) - 0 for no throttling
        const throttleBuffer = Buffer.alloc(4);
        throttleBuffer.writeInt32BE(0, 0);
        
        // Combine all parts of the body
        body = Buffer.concat([errorCodeBuffer, apiCountBuffer, apiEntriesBuffer, throttleBuffer]);
      }
    } else {
      // For non-ApiVersions requests, maintain existing behavior
      body = Buffer.alloc(0);
    }
    
    // Calculate and set message size (header size + body size)
    const messageSize = Buffer.alloc(4);
    messageSize.writeInt32BE(header.length + body.length, 0);
    
    // Combine all parts and send response
    const response = Buffer.concat([messageSize, header, body]);
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1");
