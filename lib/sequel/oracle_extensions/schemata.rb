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
	        
	    # Returns the primary key for the given table, as a hash.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
      # * <tt>:all</tt> - Returns all matching keys.  By default, returns the first matching key - provided
      #   that either there is only one key or that only the key is enabled.
      # * <tt>:first</tt> - Returns the first matching key.
	    def primary_key(table, options={})
	      sql, m = SELECT_PRIMARY_KEY_SQL, output_identifier_meth
	      table, pks = m[table], []
        pkh = Hash.new{|h,k| pks.push(h[k]=v={:table_name=>table, :columns=>[]}); v }

	      unless (z = options[:enabled]).nil?
	        sql = sql.sub /WHERE /, "WHERE c.status = #{z ? 'ENABLED' : 'DISABLED'}"
	      end

	      metadata_dataset.with_sql(sql, table.to_s.upcase).each do |r|
	        if options[:first] && pks.length==1 && r[:constraint_name] != pks[:constraint_name]
		        return pks.first
		      end

	        r = Hash[ r.map{|k,v| [k, (k==:status || v.nil? || v=='') ? v : m[v]]} ]
          pk = pkh[m.call r[:constraint_name]]
	        pk[:enabled] = r.delete(:status)=='ENABLED'
	        pk[:columns] << r.delete(:column_name)
	        pk.update r
	      end

	      unless options[:all] or (pks.length>1 and pks[0][:enabled] != pks[1][:enabled])
		      return pks.first
		    end
	      pks
	    end
    end
  end
end
