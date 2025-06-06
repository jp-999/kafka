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
  { apiKey: API_VERSIONS_KEY, minVersion: 0, maxVersion: 4 }
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
      if (apiVersion < MIN_SUPPORTED_VERSION || apiVersion > MAX_SUPPORTED_VERSION) {
        // Unsupported version - respond with error
        body = Buffer.alloc(2);
        body.writeInt16BE(UNSUPPORTED_VERSION, 0);
      } else {
        // Full ApiVersions response body format:
        // error_code (INT16) - 2 bytes
        // api_keys array length (COMPACT_ARRAY) - 1 byte for length (N+1 format)
        // api_keys entries - each 6 bytes (INT16 apiKey, INT16 minVersion, INT16 maxVersion)
        // throttle_time_ms (INT32) - 4 bytes
        // TAG_BUFFER - 1 byte (0 for empty tag buffer)
        
        // Create the body buffer
        const errorCodeBuf = Buffer.alloc(2);
        errorCodeBuf.writeInt16BE(SUCCESS, 0);
        
        // API keys array length (using COMPACT_ARRAY format, N+1)
        // We're including 1 API key, so length is 2 (1+1)
        const apiKeysLengthBuf = Buffer.alloc(1);
        apiKeysLengthBuf.writeUInt8(2, 0); // 1+1 format for COMPACT_ARRAY
        
        // API key entry for API_VERSIONS_KEY
        const apiEntryBuf = Buffer.alloc(6);
        apiEntryBuf.writeInt16BE(API_VERSIONS_KEY, 0);  // API key
        apiEntryBuf.writeInt16BE(MIN_SUPPORTED_VERSION, 2);  // Min version
        apiEntryBuf.writeInt16BE(MAX_SUPPORTED_VERSION, 4);  // Max version
        
        // Throttle time (4 bytes)
        const throttleTimeBuf = Buffer.alloc(4);
        throttleTimeBuf.writeInt32BE(0, 0);
        
        // TAG_BUFFER (1 byte for empty tag buffer)
        const tagBufferBuf = Buffer.alloc(1);
        tagBufferBuf.writeUInt8(0, 0);
        
        // Combine all parts of the body
        body = Buffer.concat([
          errorCodeBuf,
          apiKeysLengthBuf,
          apiEntryBuf,
          throttleTimeBuf,
          tagBufferBuf
        ]);
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
