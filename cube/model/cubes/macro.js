cube(`macro`, {
  sql_table: `gold.macro`,
  
  data_source: `default`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true
    },
    
    gold: {
      sql: `gold`,
      type: `number`
    },
    
    oil: {
      sql: `oil`,
      type: `number`
    },
    
    us_dollar: {
      sql: `us_dollar`,
      type: `number`
    },
    
    usd_vnd: {
      sql: `usd_vnd`,
      type: `number`
    },
    
    cpi: {
      sql: `cpi`,
      type: `number`
    },
    
    inflation_rate: {
      sql: `inflation_rate`,
      type: `number`
    },
    
    interest_rate: {
      sql: `interest_rate`,
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
