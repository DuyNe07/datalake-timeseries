cube(`indices`, {
  sql_table: `gold.indices`,
  
  data_source: `default`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true
    },
    
    russell2000: {
      sql: `russell2000`,
      type: `number`
    },
    
    dow_jones: {
      sql: `dow_jones`,
      type: `number`
    },
    
    msci_world: {
      sql: `msci_world`,
      type: `number`
    },
    
    nasdaq100: {
      sql: `nasdaq100`,
      type: `number`
    },
    
    s_p500: {
      sql: `s_p500`,
      type: `number`
    },
    
    gold: {
      sql: `gold`,
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
