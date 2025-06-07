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
      
      // Parse the request to extract topic names
      const clientIdLength = data.readInt16BE(12);
      let offset = 14 + clientIdLength;
      
      // Skip over tagBuffer
      offset += 1;
      
      // Read number of topics (in compact array format, so subtract 1)
      const topicArrayLength = data.readUInt8(offset) - 1;
      offset += 1;
      
      // Extract topic names from the request
      const topics = [];
      for (let i = 0; i < topicArrayLength; i++) {
        const topicNameLength = data.readUInt8(offset) - 1;
        offset += 1;
        const topicName = data.subarray(offset, offset + topicNameLength).toString();
        offset += topicNameLength;
        topics.push(topicName);
        
        // Skip tag buffer for the topic
        offset += 1;
      }
      
      console.log(`Requested topics: ${topics.join(', ')}`);
      
      // Build response
      // We'll create the response in parts and then combine them
      const parts = [];
      
      // Correlation ID (4 bytes)
      const correlationIdBuffer = Buffer.alloc(4);
      correlationIdBuffer.writeInt32BE(correlationId);
      parts.push(correlationIdBuffer);
      
      // Tag buffer for response (1 byte)
      parts.push(Buffer.from([0]));
      
      // Throttle time (4 bytes)
      const throttleTime = Buffer.alloc(4);
      throttleTime.writeInt32BE(0);
      parts.push(throttleTime);
      
      // Topics array length (1 byte) - in compact array format (N+1)
      parts.push(Buffer.from([topics.length + 1]));
      
      // For each topic, add the response fields
      for (const topic of topics) {
        // Error code (2 bytes) - UNKNOWN_TOPIC_OR_PARTITION (3)
        const errorCodeBuffer = Buffer.alloc(2);
        errorCodeBuffer.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION);
        parts.push(errorCodeBuffer);
        
        // Topic name length (1 byte) - in compact string format (N+1)
        const topicNameBuffer = Buffer.from(topic);
        parts.push(Buffer.from([topicNameBuffer.length + 1]));
        
        // Topic name
        parts.push(topicNameBuffer);
        
        // Topic ID (16 bytes) - all zeros for unknown topic
        parts.push(Buffer.alloc(16));
        
        // Tag buffer for topic (1 byte)
        parts.push(Buffer.from([0]));
        
        // Partitions array length (1 byte) - empty array in compact format (1)
        parts.push(Buffer.from([1]));
        
        // Topic authorized operations (4 bytes)
        const topicAuthorizedOperations = Buffer.alloc(4);
        topicAuthorizedOperations.writeInt32BE(0x0df8); // As provided in the source code
        parts.push(topicAuthorizedOperations);
        
        // Tag buffer for topic entry (1 byte)
        parts.push(Buffer.from([0]));
      }
      
      // Cursor (1 byte) - empty in compact format (0)
      parts.push(Buffer.from([0]));
      
      // Tag buffer for cursor (1 byte)
      parts.push(Buffer.from([0]));
      
      // Combine all parts into the response body
      const responseBody = Buffer.concat(parts);
      
      // Create the final response with message size
      const messageSize = Buffer.alloc(4);
      messageSize.writeInt32BE(responseBody.length);
      response = Buffer.concat([messageSize, responseBody]);
      
    } else {
      // For unsupported requests, just return a header with correlation ID
      console.log(`Unsupported API key: ${apiKey}`);
      
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
