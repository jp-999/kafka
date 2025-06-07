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
      
      // Parse client ID length
      const clientIdLength = data.readInt16BE(12);
      console.log(`Client ID length: ${clientIdLength}`);
      
      // Calculate position after client ID
      let position = 14 + clientIdLength;
      
      // Read topics array length (COMPACT_ARRAY format is N+1)
      const topicsArrayLength = data.readUInt8(position) - 1;
      console.log(`Topics array length: ${topicsArrayLength}`);
      position += 1;
      
      // Array to store topic names for the response
      const topics = [];
      
      // Parse each topic
      for (let i = 0; i < topicsArrayLength; i++) {
        // Read topic name length (COMPACT_STRING format is N+1)
        const topicNameLength = data.readUInt8(position) - 1;
        position += 1;
        
        // Read topic name
        const topicName = data.toString('utf8', position, position + topicNameLength);
        position += topicNameLength;
        
        console.log(`Topic ${i}: ${topicName} (length: ${topicNameLength})`);
        topics.push({ name: topicName, nameLength: topicNameLength });
      }
      
      // Skip partition limit and other fields - we don't need them for the response
      
      // Build response
      // First calculate total size
      let totalSize = 4 + 2 + 4 + 1;  // Message size + Error code + Throttle time + Topics array length
      
      // For each topic: 2 (error code) + 1 (topic name length) + topic name length + 16 (topic id) + 1 (partitions length) + 4 (authorized operations) + 1 (tag buffer)
      for (const topic of topics) {
        totalSize += 2 + 1 + topic.nameLength + 16 + 1 + 4 + 1;
      }
      
      // Add cursor and tag buffer
      totalSize += 1 + 1;
      
      // Create response buffer
      response = Buffer.alloc(totalSize);
      let offset = 0;
      
      // Message size
      response.writeInt32BE(totalSize - 4, offset);  // Don't include the message size field itself
      offset += 4;
      
      // Correlation ID
      response.writeInt32BE(correlationId, offset);
      offset += 4;
      
      // Throttle time (ms)
      response.writeInt32BE(0, offset);
      offset += 4;
      
      // Topics array length (COMPACT_ARRAY format is N+1)
      response.writeUInt8(topics.length + 1, offset);
      offset += 1;
      
      // For each topic
      for (const topic of topics) {
        // Error code (UNKNOWN_TOPIC_OR_PARTITION)
        response.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION, offset);
        offset += 2;
        
        // Topic name (COMPACT_STRING format is N+1)
        response.writeUInt8(topic.nameLength + 1, offset);
        offset += 1;
        
        // Topic name
        offset += response.write(topic.name, offset);
        
        // Topic ID (all zeros for unknown topic)
        for (let i = 0; i < 16; i++) {
          response.writeUInt8(0, offset);
          offset += 1;
        }
        
        // Partitions array length (empty, so just 1 in COMPACT_ARRAY format)
        response.writeUInt8(1, offset);
        offset += 1;
        
        // Topic authorized operations
        response.writeInt32BE(0x0DF8, offset);  // Standard permissions
        offset += 4;
        
        // Tag buffer (empty)
        response.writeUInt8(0, offset);
        offset += 1;
      }
      
      // Cursor (empty string in COMPACT_STRING format)
      response.writeUInt8(1, offset);  // Length 1 means empty string
      offset += 1;
      
      // Response tag buffer (empty)
      response.writeUInt8(0, offset);
    } else {
      // For non-ApiVersions requests, just return a header with correlation ID
      console.log(`Processing unsupported request with API key ${apiKey}`);
      
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
