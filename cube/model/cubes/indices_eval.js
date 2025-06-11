cube(`indices_eval`, {
  sql_table: `gold.indices_eval`,
  
  data_source: `default`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true
    },
    
    variable: {
      sql: `variable`,
      type: `string`
    },
    
    mse_srvar: {
      sql: `mse_srvar`,
      type: `number`
    },
    
    rmse_srvar: {
      sql: `rmse_srvar`,
      type: `number`
    },
    
    mae_srvar: {
      sql: `mae_srvar`,
      type: `number`
    },
    
    mse_var: {
      sql: `mse_var`,
      type: `number`
    },
    
    rmse_var: {
      sql: `rmse_var`,
      type: `number`
    },
    
    mae_var: {
      sql: `mae_var`,
      type: `number`
    }
  },
  
  measures: {
    count: {
      type: `count`
    }
  },
  
  pre_aggregations: {
    // Pre-aggregation definitions go here.
    // Learn more in the documentation: https://cube.dev/docs/caching/pre-aggregations/getting-started
  }
});
