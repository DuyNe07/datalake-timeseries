cube(`indices_future`, {
  sql_table: `gold.indices_future`,
  
  data_source: `default`,
  
  joins: {
    
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true
    },
    
    russell2000_srvar: {
      sql: `russell2000_srvar`,
      type: `number`
    },
    
    dow_jones_srvar: {
      sql: `dow_jones_srvar`,
      type: `number`
    },
    
    msci_world_srvar: {
      sql: `msci_world_srvar`,
      type: `number`
    },
    
    nasdaq100_srvar: {
      sql: `nasdaq100_srvar`,
      type: `number`
    },
    
    s_p500_srvar: {
      sql: `s_p500_srvar`,
      type: `number`
    },
    
    gold_srvar: {
      sql: `gold_srvar`,
      type: `number`
    },
    
    russell2000_varnn: {
      sql: `russell2000_varnn`,
      type: `number`
    },
    
    dow_jones_varnn: {
      sql: `dow_jones_varnn`,
      type: `number`
    },
    
    msci_world_varnn: {
      sql: `msci_world_varnn`,
      type: `number`
    },
    
    nasdaq100_varnn: {
      sql: `nasdaq100_varnn`,
      type: `number`
    },
    
    s_p500_varnn: {
      sql: `s_p500_varnn`,
      type: `number`
    },
    
    gold_varnn: {
      sql: `gold_varnn`,
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
