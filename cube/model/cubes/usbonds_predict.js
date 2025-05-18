cube(`usbonds_predict`, {
  sql_table: `gold.usbonds_predict`,
  
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
    
    us_2_year_bond_srvar: {
      sql: `us_2_year_bond_srvar`,
      type: `number`
    },
    
    us_3_month_bond_srvar: {
      sql: `us_3_month_bond_srvar`,
      type: `number`
    },
    
    us_5_year_bond_srvar: {
      sql: `us_5_year_bond_srvar`,
      type: `number`
    },
    
    us_10_year_bond_srvar: {
      sql: `us_10_year_bond_srvar`,
      type: `number`
    },
    
    gold_varnn: {
      sql: `gold_varnn`,
      type: `number`
    },
    
    us_2_year_bond_varnn: {
      sql: `us_2_year_bond_varnn`,
      type: `number`
    },
    
    us_3_month_bond_varnn: {
      sql: `us_3_month_bond_varnn`,
      type: `number`
    },
    
    us_5_year_bond_varnn: {
      sql: `us_5_year_bond_varnn`,
      type: `number`
    },
    
    us_10_year_bond_varnn: {
      sql: `us_10_year_bond_varnn`,
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
