import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Constants
const API_VERSIONS_KEY = 18;  // ApiVersions API key
const DESCRIBE_TOPIC_PARTITIONS_KEY = 75;  // DescribeTopicPartitions API key
const UNSUPPORTED_VERSION = 35;  // Error code for unsupported version
const SUCCESS = 0;  // Success code
const UNKNOWN_TOPIC_OR_PARTITION = 3; // Error code for unknown topic or partition
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
      
      // Parse the request to get the topic name
      // First, we need to find the client ID length
      const clientIdLength = data.readInt16BE(12);
      
      // Calculate the offset after the client ID
      let offset = 14 + clientIdLength;
      
      // Read the topic array length (compact array format, N+1)
      const topicsArrayLength = data.readUInt8(offset) - 1;
      offset += 1;
      
      // Process each topic (we're only expecting one for now)
      const topics = [];
      for (let i = 0; i < topicsArrayLength; i++) {
        // Read topic name length (compact string format, N+1)
        const topicNameLength = data.readUInt8(offset) - 1;
        offset += 1;
        
        // Read topic name
        const topicName = data.toString('utf8', offset, offset + topicNameLength);
        offset += topicNameLength;
        
        topics.push(topicName);
      }
      
      console.log(`Received topics: ${topics.join(', ')}`);
      
      // Prepare the response
      // We need to build a response with:
      // - Message length (4 bytes)
      // - Correlation ID (4 bytes)
      // - Throttle time (4 bytes)
      // - Topics array length (1 byte, compact array format)
      // - For each topic:
      //   - Error code (2 bytes)
      //   - Topic name (compact string)
      //   - Topic ID (16 bytes, UUID)
      //   - Partitions array (compact array, empty)
      //   - Authorized operations (4 bytes)
      //   - Tag buffer (1 byte)
      // - Cursor (1 byte)
      // - Tag buffer (1 byte)
      
      // Calculate the size of the response
      let responseSize = 0;
      responseSize += 4; // Message length
      responseSize += 4; // Correlation ID
      responseSize += 4; // Throttle time
      responseSize += 1; // Topics array length
      
      // For each topic
      const topicResponses = [];
      for (const topic of topics) {
        const topicResponse = {
          errorCode: Buffer.alloc(2),
          topicName: Buffer.from(topic),
          topicNameLength: Buffer.alloc(1),
          topicId: Buffer.alloc(16),
          partitionsLength: Buffer.alloc(1),
          authorizedOperations: Buffer.alloc(4),
          tagBuffer: Buffer.alloc(1)
        };
        
        // Set values
        topicResponse.errorCode.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION, 0);
        topicResponse.topicNameLength.writeUInt8(topic.length + 1, 0); // Compact string format (length + 1)
        // Topic ID is all zeros for unknown topic
        topicResponse.partitionsLength.writeUInt8(0 + 1, 0); // Compact array format (0 elements + 1)
        topicResponse.authorizedOperations.writeInt32BE(0x0df8, 0); // Default authorized operations
        topicResponse.tagBuffer.writeUInt8(0, 0); // No tagged fields
        
        topicResponses.push(topicResponse);
        
        // Calculate size for this topic
        responseSize += 2; // Error code
        responseSize += 1; // Topic name length
        responseSize += topic.length; // Topic name
        responseSize += 16; // Topic ID
        responseSize += 1; // Partitions array length
        responseSize += 4; // Authorized operations
        responseSize += 1; // Tag buffer
      }
      
      responseSize += 1; // Cursor
      responseSize += 1; // Tag buffer
      
      // Allocate buffer for response
      response = Buffer.alloc(responseSize);
      
      // Write response
      let responseOffset = 0;
      
      // Message size (4 bytes) - size of the response minus the size field itself
      response.writeInt32BE(responseSize - 4, responseOffset);
      responseOffset += 4;
      
      // Correlation ID (4 bytes) - from the request
      response.writeInt32BE(correlationId, responseOffset);
      responseOffset += 4;
      
      // Throttle time (4 bytes) - 0
      response.writeInt32BE(0, responseOffset);
      responseOffset += 4;
      
      // Topics array length (1 byte, compact array format)
      response.writeUInt8(topics.length + 1, responseOffset);
      responseOffset += 1;
      
      // Write each topic response
      for (const topicResponse of topicResponses) {
        // Error code (2 bytes)
        topicResponse.errorCode.copy(response, responseOffset);
        responseOffset += 2;
        
        // Topic name length (1 byte, compact string format)
        topicResponse.topicNameLength.copy(response, responseOffset);
        responseOffset += 1;
        
        // Topic name
        topicResponse.topicName.copy(response, responseOffset);
        responseOffset += topicResponse.topicName.length;
        
        // Topic ID (16 bytes, UUID)
        topicResponse.topicId.copy(response, responseOffset);
        responseOffset += 16;
        
        // Partitions array length (1 byte, compact array format)
        topicResponse.partitionsLength.copy(response, responseOffset);
        responseOffset += 1;
        
        // Authorized operations (4 bytes)
        topicResponse.authorizedOperations.copy(response, responseOffset);
        responseOffset += 4;
        
        // Tag buffer (1 byte)
        topicResponse.tagBuffer.copy(response, responseOffset);
        responseOffset += 1;
      }
      
      // Cursor (1 byte) - 0 for no cursor
      response.writeUInt8(0, responseOffset);
      responseOffset += 1;
      
      // Tag buffer (1 byte) - 0 for no tagged fields
      response.writeUInt8(0, responseOffset);
    } else {
      // For other non-implemented requests, just return a header with correlation ID
      console.log("Processing non-implemented request");
      
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
