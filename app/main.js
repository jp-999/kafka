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
      
      // Parse the request to get the topic name
      // Skip 12 bytes for header (4 size + 2 api key + 2 api version + 4 correlation id)
      let reqOffset = 12;
      
      // Skip client id (nullable string)
      const clientIdSize = data.readInt16BE(reqOffset);
      reqOffset += 2;
      if (clientIdSize !== -1) {
        reqOffset += clientIdSize;
      }
      
      // Skip tag buffer
      const tagBufferSize = data.readUInt8(reqOffset);
      reqOffset += 1;
      if (tagBufferSize > 0) {
        reqOffset += tagBufferSize - 1; // -1 because we already read one byte for size
      }
      
      // Read topics array length
      const topicsLength = data.readInt32BE(reqOffset);
      reqOffset += 4;
      
      // Get the first topic name
      let topicName = '';
      if (topicsLength > 0) {
        const topicNameLength = data.readInt16BE(reqOffset);
        reqOffset += 2;
        
        if (topicNameLength > 0) {
          topicName = data.toString('utf8', reqOffset, reqOffset + topicNameLength);
          reqOffset += topicNameLength;
        }
      }
      
      console.log(`Topic requested: "${topicName}"`);
      
      // Prepare response for an unknown topic
      // Calculate response size:
      // 4 bytes for throttle_time_ms
      // 2 bytes for error code
      // 2 bytes for topic name length
      // X bytes for topic name
      // 16 bytes for topic_id (UUID)
      // 4 bytes for partitions array length (0)
      // 1 byte for tag buffer
      const responseSize = 4 + 2 + 2 + topicName.length + 16 + 4 + 1;
      
      response = Buffer.alloc(4 + 4 + responseSize); // 4 message size + 4 correlation id + response body
      let offset = 0;
      
      // Message size (4 bytes)
      response.writeInt32BE(4 + responseSize, offset);
      offset += 4;
      
      // Correlation ID (4 bytes)
      response.writeInt32BE(correlationId, offset);
      offset += 4;
      
      // Throttle time (4 bytes) - 0
      response.writeInt32BE(0, offset);
      offset += 4;
      
      // Error code (2 bytes) - UNKNOWN_TOPIC_OR_PARTITION
      response.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION, offset);
      offset += 2;
      
      // Topic name (string)
      response.writeInt16BE(topicName.length, offset);
      offset += 2;
      Buffer.from(topicName).copy(response, offset);
      offset += topicName.length;
      
      // Topic ID (UUID) - 00000000-0000-0000-0000-000000000000
      Buffer.from([
        0, 0, 0, 0,  // First 4 bytes of UUID
        0, 0,        // Next 2 bytes
        0, 0,        // Next 2 bytes
        0, 0,        // Next 2 bytes
        0, 0, 0, 0, 0, 0 // Last 6 bytes
      ]).copy(response, offset);
      offset += 16;
      
      // Partitions array (empty)
      response.writeInt32BE(0, offset); // 0 partitions
      offset += 4;
      
      // Tag buffer (empty)
      response.writeUInt8(0, offset); // No tagged fields
    } else {
      // For other requests, just return a header with correlation ID
      console.log("Processing unsupported request");
      
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
