module.exports = {
  // Database type, host, port, and catalog/database name will be primarily
  // configured via environment variables (CUBEJS_DB_TYPE, CUBEJS_DB_HOST, 
  // CUBEJS_DB_PORT, CUBEJS_DB_TRINO_CATALOG).

  // API Secret will be read from the CUBEJS_API_SECRET environment variable.
  apiSecret: process.env.CUBEJS_API_SECRET,

  // Path to your data schema files (relative to this cube.js file)
  // Host: /home/DuyBao/cube/model/ -> Container: /cube/conf/model/
  schemaPath: 'model',

  // Enable Cube.js Dev Server. The port is typically 4000 by default
  // or can be set with CUBEJS_DEV_PORT.
  devServer: true,

  // Telemetry can be enabled or disabled.
  telemetry: true,
  
  // Cấu hình CORS để các client như PowerBI có thể kết nối
  http: {
    cors: {
      origin: '*', // Cho phép tất cả các nguồn (trong môi trường production nên giới hạn)
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization', 'x-turbo-mode']
    }
  }
};