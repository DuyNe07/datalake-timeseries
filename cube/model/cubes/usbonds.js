cube(`usbonds`, {
  sql_table: `gold.usbonds`,
  
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
    
    us_2_year_bond: {
      sql: `us_2_year_bond`,
      type: `number`
    },
    
    us_3_month_bond: {
      sql: `us_3_month_bond`,
      type: `number`
    },
    
    us_5_year_bond: {
      sql: `us_5_year_bond`,
      type: `number`
    },
    
    us_10_year_bond: {
      sql: `us_10_year_bond`,
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
