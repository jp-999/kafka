import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Constants
const API_VERSIONS_KEY = 18;  // ApiVersions API key
const DESCRIBE_TOPIC_PARTITIONS_KEY = 75;  // DescribeTopicPartitions API key
const UNSUPPORTED_VERSION = 35;  // Error code for unsupported version
const SUCCESS = 0;  // Success code
const UNKNOWN_TOPIC_OR_PARTITION = 3;  // Error code for unknown topic or partition
const MAX_SUPPORTED_VERSION_API_VERSIONS = 4;  // Maximum supported version for ApiVersions
const MIN_SUPPORTED_VERSION_API_VERSIONS = 0;  // Minimum supported version for ApiVersions
const MAX_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS = 0;  // Maximum supported version for DescribeTopicPartitions
const MIN_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS = 0;  // Minimum supported version for DescribeTopicPartitions

// Helper function to send response
const sendResponseMessage = (connection, buffer) => {
  connection.write(buffer);
};

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
      
      // Create response with two API entries (APIVersions and DescribeTopicPartitions)
      // Each API entry is 6 bytes (2 for key, 2 for min version, 2 for max version) + 1 byte for tag buffer
      // Total size increases by 7 bytes for the additional API entry
      response = Buffer.alloc(30);  // 23 (previous size) + 7 (new API entry)
      let offset = 0;
      
      // Message size (4 bytes) - 26 bytes for the rest of the message (19 + 7)
      response.writeInt32BE(26, offset);
      offset += 4;
      
      // Correlation ID (4 bytes) - from the request
      response.writeInt32BE(correlationId, offset);
      offset += 4;
      
      // Check if API version is supported
      if (apiVersion < MIN_SUPPORTED_VERSION_API_VERSIONS || apiVersion > MAX_SUPPORTED_VERSION_API_VERSIONS) {
        // Unsupported version - respond with error code
        console.log(`Unsupported version ${apiVersion}, responding with error code ${UNSUPPORTED_VERSION}`);
        response.writeInt16BE(UNSUPPORTED_VERSION, offset);
      } else {
        // Supported version - respond with success
        console.log(`Supported version ${apiVersion}, responding with success`);
        response.writeInt16BE(SUCCESS, offset);
      }
      offset += 2;
      
      // API keys array length (1 byte) - 3 in COMPACT_ARRAY format (N+1) for 2 entries
      response.writeUInt8(3, offset);
      offset += 1;
      
      // First API key entry (ApiVersions)
      response.writeInt16BE(API_VERSIONS_KEY, offset);  // API key (18 = API_VERSIONS)
      offset += 2;
      response.writeInt16BE(MIN_SUPPORTED_VERSION_API_VERSIONS, offset);   // Min version (0)
      offset += 2;
      response.writeInt16BE(MAX_SUPPORTED_VERSION_API_VERSIONS, offset);   // Max version (4)
      offset += 2;
      
      // First API key entry tag buffer (1 byte) - 0 = no tagged fields
      response.writeUInt8(0, offset);
      offset += 1;
      
      // Second API key entry (DescribeTopicPartitions)
      response.writeInt16BE(DESCRIBE_TOPIC_PARTITIONS_KEY, offset);  // API key (75 = DESCRIBE_TOPIC_PARTITIONS)
      offset += 2;
      response.writeInt16BE(MIN_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS, offset);   // Min version (0)
      offset += 2;
      response.writeInt16BE(MAX_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS, offset);   // Max version (0)
      offset += 2;
      
      // Second API key entry tag buffer (1 byte) - 0 = no tagged fields
      response.writeUInt8(0, offset);
      offset += 1;
      
      // Throttle time (4 bytes) - 0
      response.writeInt32BE(0, offset);
      offset += 4;
      
      // Response tag buffer (1 byte) - 0 = no tagged fields
      response.writeUInt8(0, offset);
    } else if (apiKey === DESCRIBE_TOPIC_PARTITIONS_KEY) {
      // Handle DescribeTopicPartitions request
      console.log("Processing DescribeTopicPartitions request");
      
      // Check if API version is supported
      if (apiVersion < MIN_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS || 
          apiVersion > MAX_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS) {
        // Unsupported version - respond with simple header and error code
        response = Buffer.alloc(8);
        response.writeInt32BE(4, 0); // Message size (4 bytes)
        response.writeInt32BE(correlationId, 4); // Correlation ID (4 bytes)
      } else {
        // Parse the topic name from the request
        const clientIdLength = data.readInt16BE(12);
        // Skip clientId bytes to get to the topics section
        let offset = 14 + clientIdLength;
        
        // Read topic array length (compact array format N+1)
        const topicArrayLength = data.readUInt8(offset) - 1;
        offset += 1;
        
        // For each topic in the request, read its name
        const topics = [];
        for (let i = 0; i < topicArrayLength; i++) {
          const topicNameLength = data.readUInt8(offset);
          offset += 1;
          const topicName = data.subarray(offset, offset + topicNameLength).toString();
          offset += topicNameLength;
          topics.push(topicName);
        }
        
        // Create response buffer (we'll calculate the size first)
        // Start with fixed parts of the response
        let responseBufferSize = 4 + // Message size
                                4 + // Correlation ID
                                4 + // Throttle time
                                1;  // Topics array length (compact array)
        
        // For each topic:
        const topicResponses = [];
        for (const topicName of topics) {
          // For each topic we need:
          const topicResponseBuffers = [
            Buffer.from([0, UNKNOWN_TOPIC_OR_PARTITION]),  // Error code (INT16)
            Buffer.from([topicName.length]),               // Topic name length (COMPACT_STRING length)
            Buffer.from(topicName, 'utf8'),                // Topic name (COMPACT_STRING data)
            Buffer.alloc(16, 0),                          // Topic ID (UUID - all zeros for unknown topic)
            Buffer.from([0]),                              // Partitions array length (compact array - 0 partitions)
            Buffer.from([0, 0, 0, 0xf8]),                  // Topic authorized operations (INT32)
            Buffer.from([0])                               // Topic tag buffer (TAGGED_FIELDS)
          ];
          
          const topicResponseBuffer = Buffer.concat(topicResponseBuffers);
          topicResponses.push(topicResponseBuffer);
          responseBufferSize += topicResponseBuffer.length;
        }
        
        // Add the cursor and tag buffer
        responseBufferSize += 1 + // Cursor (Compact array of zero size)
                             1;  // Response tag buffer (TAGGED_FIELDS)
        
        // Create the final response buffer
        response = Buffer.alloc(responseBufferSize);
        let writeOffset = 0;
        
        // Write message size (total size minus the 4 bytes for size field)
        response.writeInt32BE(responseBufferSize - 4, writeOffset);
        writeOffset += 4;
        
        // Write correlation ID
        response.writeInt32BE(correlationId, writeOffset);
        writeOffset += 4;
        
        // Write throttle time (0 ms)
        response.writeInt32BE(0, writeOffset);
        writeOffset += 4;
        
        // Write topics array length (compact format N+1)
        response.writeUInt8(topics.length + 1, writeOffset);
        writeOffset += 1;
        
        // Write each topic response
        for (const topicBuffer of topicResponses) {
          topicBuffer.copy(response, writeOffset);
          writeOffset += topicBuffer.length;
        }
        
        // Write cursor (0 indicates empty array in compact format)
        response.writeUInt8(0, writeOffset);
        writeOffset += 1;
        
        // Write response tag buffer (empty tagged fields)
        response.writeUInt8(0, writeOffset);
      }
    } else {
      // For other non-implemented requests, just return a header with correlation ID
      console.log(`Processing non-implemented request with API key: ${apiKey}`);
      
      response = Buffer.alloc(8);
      
      // Message size (4 bytes) - 4 bytes for the header
      response.writeInt32BE(4, 0);
      
      // Correlation ID (4 bytes) - from the request
      response.writeInt32BE(correlationId, 4);
    }
    
    // Send the response
    console.log(`Sending response of ${response.length} bytes`);
    sendResponseMessage(connection, response);
  });
});

server.listen(9092, "127.0.0.1", () => {
  console.log("Kafka server listening on port 9092");
});
