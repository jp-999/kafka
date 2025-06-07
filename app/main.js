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
      
      // Extract client ID length
      const clientIdLength = data.readInt16BE(12);
      console.log(`Client ID length: ${clientIdLength}`);
      
      // We need to skip over the client ID
      // Client ID starts at position 14, so position after client ID is 14 + clientIdLength
      let position = 14 + clientIdLength;
      
      // Create a buffer array for the response parts
      const responseParts = [];
      
      // Add correlation ID (4 bytes)
      const correlationIdBuf = Buffer.alloc(4);
      correlationIdBuf.writeInt32BE(correlationId);
      responseParts.push(correlationIdBuf);
      
      // Add throttle time (4 bytes) - 0ms
      const throttleTimeBuf = Buffer.alloc(4);
      throttleTimeBuf.writeInt32BE(0);
      responseParts.push(throttleTimeBuf);
      
      // Parse the topic data from request
      try {
        // Check if we have enough data to read the topics array length
        if (position >= data.length) {
          console.log("Request format error: data too short to read topics array length");
          throw new Error("Invalid request format");
        }
        
        // Read the topics array length (COMPACT_ARRAY format is N+1)
        const topicsArrayLengthByte = data.readUInt8(position);
        const topicsArrayLength = topicsArrayLengthByte > 0 ? topicsArrayLengthByte - 1 : 0;
        console.log(`Topics array length byte: ${topicsArrayLengthByte}, actual length: ${topicsArrayLength}`);
        position++;
        
        // Add topics array length to response (COMPACT_ARRAY format is N+1)
        // We'll always have at least one topic in the response
        const topicsLengthBuf = Buffer.alloc(1);
        topicsLengthBuf.writeUInt8(topicsArrayLength > 0 ? topicsArrayLength + 1 : 2); // At least 2 for one topic
        responseParts.push(topicsLengthBuf);
        
        // Parse each topic
        for (let i = 0; i < topicsArrayLength; i++) {
          // Check if we have enough data
          if (position >= data.length) {
            console.log(`Request format error: data too short to read topic ${i}`);
            throw new Error("Invalid request format");
          }
          
          // Read topic name length (COMPACT_STRING format is N+1)
          const topicNameLengthByte = data.readUInt8(position);
          const topicNameLength = topicNameLengthByte > 0 ? topicNameLengthByte - 1 : 0;
          position++;
          
          // Check if we have enough data
          if (position + topicNameLength > data.length) {
            console.log(`Request format error: data too short to read topic name of length ${topicNameLength}`);
            throw new Error("Invalid request format");
          }
          
          // Read topic name
          const topicName = data.toString('utf8', position, position + topicNameLength);
          position += topicNameLength;
          
          console.log(`Topic ${i}: ${topicName} (length: ${topicNameLength})`);
          
          // Add error code (UNKNOWN_TOPIC_OR_PARTITION)
          const errorCodeBuf = Buffer.alloc(2);
          errorCodeBuf.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION);
          responseParts.push(errorCodeBuf);
          
          // Add topic name (COMPACT_STRING format is N+1)
          const topicNameLenBuf = Buffer.alloc(1);
          topicNameLenBuf.writeUInt8(topicNameLength + 1);
          responseParts.push(topicNameLenBuf);
          
          if (topicNameLength > 0) {
            const topicNameBuf = Buffer.from(topicName);
            responseParts.push(topicNameBuf);
          }
          
          // Add topic ID (all zeros for unknown topic)
          const topicIdBuf = Buffer.alloc(16).fill(0);
          responseParts.push(topicIdBuf);
          
          // Add partitions array length (empty, so just 1 in COMPACT_ARRAY format)
          const partitionsLengthBuf = Buffer.alloc(1);
          partitionsLengthBuf.writeUInt8(1);
          responseParts.push(partitionsLengthBuf);
          
          // Add topic authorized operations
          const authOpsBuf = Buffer.alloc(4);
          authOpsBuf.writeInt32BE(0x0DF8);  // Standard permissions
          responseParts.push(authOpsBuf);
          
          // Add tag buffer (empty)
          const tagBufTopic = Buffer.alloc(1);
          tagBufTopic.writeUInt8(0);
          responseParts.push(tagBufTopic);
        }
        
        // If no topics were parsed, add a default topic response with a null name
        if (topicsArrayLength === 0) {
          console.log("No topics in request, adding default topic response");
          
          // Add error code (UNKNOWN_TOPIC_OR_PARTITION)
          const errorCodeBuf = Buffer.alloc(2);
          errorCodeBuf.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION);
          responseParts.push(errorCodeBuf);
          
          // Add topic name (empty string in COMPACT_STRING format)
          const topicNameLenBuf = Buffer.alloc(1);
          topicNameLenBuf.writeUInt8(1); // Length 1 means empty string
          responseParts.push(topicNameLenBuf);
          
          // Add topic ID (all zeros for unknown topic)
          const topicIdBuf = Buffer.alloc(16).fill(0);
          responseParts.push(topicIdBuf);
          
          // Add partitions array length (empty, so just 1 in COMPACT_ARRAY format)
          const partitionsLengthBuf = Buffer.alloc(1);
          partitionsLengthBuf.writeUInt8(1);
          responseParts.push(partitionsLengthBuf);
          
          // Add topic authorized operations
          const authOpsBuf = Buffer.alloc(4);
          authOpsBuf.writeInt32BE(0x0DF8);  // Standard permissions
          responseParts.push(authOpsBuf);
          
          // Add tag buffer (empty)
          const tagBufTopic = Buffer.alloc(1);
          tagBufTopic.writeUInt8(0);
          responseParts.push(tagBufTopic);
        }
      } catch (err) {
        console.log(`Error parsing request: ${err.message}`);
        // Even if there's an error, we'll provide a response with an unknown topic
        
        // If topics array length hasn't been added, add it
        if (responseParts.length === 2) {
          // Add topics array length (1 topic in COMPACT_ARRAY format)
          const topicsLengthBuf = Buffer.alloc(1);
          topicsLengthBuf.writeUInt8(2); // 2 for one topic
          responseParts.push(topicsLengthBuf);
          
          // Add error code (UNKNOWN_TOPIC_OR_PARTITION)
          const errorCodeBuf = Buffer.alloc(2);
          errorCodeBuf.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION);
          responseParts.push(errorCodeBuf);
          
          // Add topic name (empty string in COMPACT_STRING format)
          const topicNameLenBuf = Buffer.alloc(1);
          topicNameLenBuf.writeUInt8(1); // Length 1 means empty string
          responseParts.push(topicNameLenBuf);
          
          // Add topic ID (all zeros for unknown topic)
          const topicIdBuf = Buffer.alloc(16).fill(0);
          responseParts.push(topicIdBuf);
          
          // Add partitions array length (empty, so just 1 in COMPACT_ARRAY format)
          const partitionsLengthBuf = Buffer.alloc(1);
          partitionsLengthBuf.writeUInt8(1);
          responseParts.push(partitionsLengthBuf);
          
          // Add topic authorized operations
          const authOpsBuf = Buffer.alloc(4);
          authOpsBuf.writeInt32BE(0x0DF8);  // Standard permissions
          responseParts.push(authOpsBuf);
          
          // Add tag buffer (empty)
          const tagBufTopic = Buffer.alloc(1);
          tagBufTopic.writeUInt8(0);
          responseParts.push(tagBufTopic);
        }
      }
      
      // Add cursor (empty string in COMPACT_STRING format)
      const cursorBuf = Buffer.alloc(1);
      cursorBuf.writeUInt8(1);  // Length 1 means empty string
      responseParts.push(cursorBuf);
      
      // Add response tag buffer (empty)
      const tagBuf = Buffer.alloc(1);
      tagBuf.writeUInt8(0);
      responseParts.push(tagBuf);
      
      // Concatenate all parts
      const responseBody = Buffer.concat(responseParts);
      
      // Calculate total size
      const messageSize = responseBody.length;
      
      // Create the complete response with message size
      const messageSizeBuf = Buffer.alloc(4);
      messageSizeBuf.writeInt32BE(messageSize);
      
      response = Buffer.concat([messageSizeBuf, responseBody]);
      
      console.log(`Built DescribeTopicPartitions response of ${response.length} bytes`);
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
