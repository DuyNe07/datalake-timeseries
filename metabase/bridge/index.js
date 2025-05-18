const express = require('express');
const { CubejsApi } = require('@cubejs-client/core');
const axios = require('axios');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Configure Cube.js API Client
const cubeApi = new CubejsApi(process.env.CUBEJS_API_SECRET || 'DuyBao21133', {
  apiUrl: process.env.CUBEJS_API_URL || 'http://cube:4000/cubejs-api/v1'
});

// Root endpoint - health check
app.get('/', (req, res) => {
  res.json({ status: 'OK', message: 'Cube-Metabase bridge is running' });
});

// Get available cubes and their dimensions
app.get('/api/meta', async (req, res) => {
  try {
    const metaResponse = await cubeApi.meta();
    res.json(metaResponse);
  } catch (error) {
    console.error('Error fetching meta:', error);
    res.status(500).json({ error: error.toString() });
  }
});

// Run a query against Cube.js
app.post('/api/query', async (req, res) => {
  try {
    const queryResponse = await cubeApi.load(req.body);
    res.json(queryResponse);
  } catch (error) {
    console.error('Error running query:', error);
    res.status(500).json({ error: error.toString() });
  }
});

// Send a query directly to Cube.js REST API
app.post('/api/proxy', async (req, res) => {
  try {
    const response = await axios.post(
      `${process.env.CUBEJS_API_URL || 'http://cube:4000/cubejs-api/v1'}/load`,
      req.body,
      {
        headers: {
          Authorization: `${process.env.CUBEJS_API_SECRET || 'DuyBao21133'}`
        }
      }
    );
    res.json(response.data);
  } catch (error) {
    console.error('Error proxying to Cube.js:', error);
    res.status(500).json({ error: error.toString() });
  }
});

// Get dimensions from a specific cube
app.get('/api/cubes/:cubeName', async (req, res) => {
  try {
    const { cubeName } = req.params;
    const metaResponse = await cubeApi.meta();
    
    const cube = metaResponse.cubes.find(c => c.name === cubeName);
    if (!cube) {
      return res.status(404).json({ error: `Cube '${cubeName}' not found` });
    }
    
    res.json(cube);
  } catch (error) {
    console.error(`Error fetching cube ${req.params.cubeName}:`, error);
    res.status(500).json({ error: error.toString() });
  }
});

const PORT = process.env.PORT || 3500;
app.listen(PORT, () => {
  console.log(`Cube-Metabase bridge running on port ${PORT}`);
});
