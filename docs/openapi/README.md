# OpenAPI Schemas

This directory contains automatically generated OpenAPI (Swagger) schemas for the Beta9 gRPC Gateway endpoints.

## Generated Schemas

- `pod.swagger.json` - Pod service API endpoints
- `image.swagger.json` - Image service API endpoints  
- `gateway.swagger.json` - Gateway service API endpoints

## How to Generate

To regenerate the OpenAPI schemas, run:

```bash
make openapi
```

Or to regenerate all protocol buffers including OpenAPI:

```bash
make protocol
```

## Viewing the Schemas

You can view these schemas using:

1. **Swagger UI**: Upload the JSON files to [Swagger Editor](https://editor.swagger.io/)
2. **Local Swagger UI**: Use tools like `swagger-ui-serve` or similar
3. **API Documentation Tools**: Import into tools like Postman, Insomnia, etc.

## API Endpoints

The schemas include all the HTTP endpoints that gRPC Gateway exposes, including:

- **Pod Service**: Container management, file operations, process control
- **Image Service**: Container image operations
- **Gateway Service**: Core gateway functionality

## Notes

- These schemas are automatically generated from `.proto` files
- The HTTP annotations in the proto files define the REST endpoints
- All endpoints support the same authentication and authorization as the gRPC services
- The schemas include request/response models, parameter validation, and error responses 