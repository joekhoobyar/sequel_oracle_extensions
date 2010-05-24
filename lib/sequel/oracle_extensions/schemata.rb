require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The oracle_schemata extension adds some schema related methods to the Oracle database adapater.
module Sequel
  module Oracle
    module DatabaseMethods
      
			SELECT_INDEXES_SQL = %q{
SELECT i.index_name, i.status, i.uniqueness, ic.column_name
FROM all_indexes i
INNER JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name 
WHERE i.table_name = ? AND i.dropped = 'NO' AND NOT EXISTS (
	SELECT uc.index_name FROM all_constraints uc
	WHERE uc.index_name = i.index_name AND uc.owner = i.owner AND uc.constraint_type = 'P'
)
ORDER BY status DESC, index_name, ic.column_position
		  }.freeze
      
      SELECT_PRIMARY_KEY_SQL = %q{
SELECT c.constraint_name, c.index_name, c.status, cc.column_name
FROM all_constraints c
INNER JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
WHERE c.table_name = ? AND c.constraint_type = 'P'
ORDER BY status DESC, constraint_name, cc.position
      }.freeze

	    # Returns the indexes for the given table, excluding any primary keys.
	    def indexes(table)
		    m = output_identifier_meth
	      table = m[table]
		    ixs = Hash.new{|h,k| h[k] = {:table_name=>table, :columns=>[]}}
	      metadata_dataset.with_sql(SELECT_INDEXES_SQL, table.to_s.upcase).each do |r|
	        r = Hash[ r.map{|k,v| [k, (k==:index_name || k==:column_name) ? m[v] : v]} ]
		      ix = ixs[m.call r.delete(:index_name)]
	        ix[:valid] = r.delete(:status)=='VALID'
	        ix[:unique] = r.delete(:uniqueness)=='UNIQUE'
	        ix[:columns] << r.delete(:column_name)
	        ix.update r
	      end
	      ixs
	    end
	        
	    # Returns the primary key for the given table.
	    def primary_key(table)
		    m = output_identifier_meth
	      table = m[table]
        pks = Hash.new{|h,k| h[k] = {:table_name=>table, :columns=>[]}}
	      metadata_dataset.with_sql(SELECT_PRIMARY_KEY_SQL, table.to_s.upcase).each do |r|
	        r = Hash[ r.map{|k,v| [k, (k==:status || v.nil? || v=='') ? v : m[v]]} ]
          pk = pks[m.call r.delete(:constraint_name)]
	        pk[:enabled] = r.delete(:status)=='ENABLED'
	        pk[:columns] << r.delete(:column_name)
	        pk.update r
	      end
	      return pks.first.last if pks.size <= 1 or pks[0][:enabled] == pks[1][:enabled]
	      pks
	    end
    end
  end
end
