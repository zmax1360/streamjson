# JSON Stream Processor

A Scala application that efficiently processes large JSON data using streaming to avoid memory issues. The program uses Vert.x for asynchronous operations and Jackson for JSON parsing. It can process JSON from either local files or API endpoints.

## Features

- Supports both local file and API endpoint JSON processing
- Reads local files in configurable chunks (default 1MB)
- Streams API responses efficiently
- Handles incomplete JSON objects split across chunks
- Processes JSON objects one at a time to minimize memory usage
- Uses asynchronous I/O operations with Vert.x
- Graceful error handling

## Prerequisites

- Java 11 or higher
- Gradle

## Building the Project

```bash
gradle build
```

## Running the Program

For processing a local JSON file:
```bash
gradle run --args="path/to/your/large.json"
```

For processing JSON from an API endpoint:
```bash
gradle run -PmainClass=com.example.jsonstream.ApiJsonStreamProcessor --args="https://api.example.com/data"
```

## How it Works

### Local File Processing
1. The program opens the specified JSON file using Vert.x's asynchronous file system API
2. It reads the file in chunks of 1MB (configurable in the code)
3. Each chunk is processed to extract complete JSON objects
4. If a JSON object is split across chunks, it's buffered until the complete object is available
5. Each complete JSON object is processed individually

### API Processing
1. The program makes an HTTP request to the specified API endpoint using Vert.x WebClient
2. It receives the response as a stream of data
3. The data is processed in chunks as it arrives
4. Complete JSON objects are extracted and processed individually
5. Handles HTTP-specific concerns like SSL, compression, and status codes

## Customization

To modify the processing logic, update the `processJsonObject` method in either:
- `JsonStreamParser` class for file processing
- `ApiJsonStreamParser` class for API processing

These methods receive each complete JSON object as a Jackson `JsonNode` which you can process according to your needs.

## Error Handling

The program includes error handling for:
- File not found
- Invalid JSON format
- Incomplete JSON objects
- I/O errors
- API connection issues
- HTTP error responses
- SSL/TLS issues

## Dependencies

- Scala 2.13.10
- Vert.x Core 4.4.4
- Vert.x Web Client 4.4.4
- Jackson Core 2.15.2
- Jackson Scala Module 2.15.2 