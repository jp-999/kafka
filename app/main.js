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
const SUPPORTED_APIS = [
  { apiKey: API_VERSIONS_KEY, minVersion: MIN_SUPPORTED_VERSION, maxVersion: MAX_SUPPORTED_VERSION }
];

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
      
      // Check if version is supported (must be between 0 and 4 inclusive)
      if (apiVersion < MIN_SUPPORTED_VERSION || apiVersion > MAX_SUPPORTED_VERSION) {
        // Unsupported version - just return error code
        body = Buffer.alloc(2);
        body.writeInt16BE(UNSUPPORTED_VERSION, 0);
      } else {
        // Create the ApiVersions response body according to v3/v4 format:
        // - error_code (int16)
        // - api_versions array
        //   - array length (int16)
        //   - for each API: apiKey (int16), minVersion (int16), maxVersion (int16)
        // - throttle_time_ms (int32)
        
        const apiCount = SUPPORTED_APIS.length;
        const bodySize = 2 + 2 + (apiCount * 6) + 4; // error_code + array_length + APIs + throttle_time
        
        body = Buffer.alloc(bodySize);
        let offset = 0;
        
        // Write error code (SUCCESS)
        body.writeInt16BE(SUCCESS, offset);
        offset += 2;
        
        // Write array length
        body.writeInt16BE(apiCount, offset);
        offset += 2;
        
        // Write each API version info
        for (const api of SUPPORTED_APIS) {
          body.writeInt16BE(api.apiKey, offset);
          offset += 2;
          body.writeInt16BE(api.minVersion, offset);
          offset += 2;
          body.writeInt16BE(api.maxVersion, offset);
          offset += 2;
        }
        
        // Write throttle_time_ms (0 for no throttling)
        body.writeInt32BE(0, offset);
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
