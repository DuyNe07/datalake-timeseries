cube(`macro_future`, {
  sql_table: `gold.macro_future`,
  
  data_source: `default`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true
    },
    
    gold_srvar: {
      sql: `gold_srvar`,
      type: `number`
    },
    
    oil_srvar: {
      sql: `oil_srvar`,
      type: `number`
    },
    
    us_dollar_srvar: {
      sql: `us_dollar_srvar`,
      type: `number`
    },
    
    usd_vnd_srvar: {
      sql: `usd_vnd_srvar`,
      type: `number`
    },
    
    cpi_srvar: {
      sql: `cpi_srvar`,
      type: `number`
    },
    
    inflation_rate_srvar: {
      sql: `inflation_rate_srvar`,
      type: `number`
    },
    
    interest_rate_srvar: {
      sql: `interest_rate_srvar`,
      type: `number`
    },
    
    gold_varnn: {
      sql: `gold_varnn`,
      type: `number`
    },
    
    oil_varnn: {
      sql: `oil_varnn`,
      type: `number`
    },
    
    us_dollar_varnn: {
      sql: `us_dollar_varnn`,
      type: `number`
    },
    
    usd_vnd_varnn: {
      sql: `usd_vnd_varnn`,
      type: `number`
    },
    
    cpi_varnn: {
      sql: `cpi_varnn`,
      type: `number`
    },
    
    inflation_rate_varnn: {
      sql: `inflation_rate_varnn`,
      type: `number`
    },
    
    interest_rate_varnn: {
      sql: `interest_rate_varnn`,
      type: `number`
    },
    
    date: {
      sql: `date`,
      type: `time`
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
