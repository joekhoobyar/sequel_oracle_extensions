require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The oracle_schemata extension adds some schema related methods to the Oracle database adapater.
module Sequel
  module Oracle
    module DatabaseMethods
      
			SELECT_INDEXES_SQL = %q{
SELECT i.index_name, i.status, i.uniqueness, ic.column_name
FROM all_indexes i
	INNER JOIN all_ind_columns ic
		ON ic.index_owner = i.owner AND ic.index_name = i.index_name 
WHERE i.table_name = ? AND i.dropped = 'NO'
ORDER BY status DESC, index_name, ic.column_position
		  }.freeze
      
      SELECT_PRIMARY_KEY_SQL = %q{
SELECT c.constraint_name, c.index_name, c.status, c.validated, c.rely, cc.column_name
FROM all_constraints c
	INNER JOIN all_cons_columns cc
		ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
WHERE c.table_name = ? AND c.constraint_type = 'P'
ORDER BY status DESC, constraint_name, cc.position
      }.freeze

      SELECT_FOREIGN_KEYS_SQL = %q{
SELECT f.constraint_name, f.index_name, f.status, f.validated, f.rely, fc.column_name
FROM all_constraints f
	INNER JOIN all_cons_columns fc
		ON fc.owner = f.owner AND fc.constraint_name = f.constraint_name
WHERE f.table_name = ? AND f.constraint_type = 'R'
ORDER BY status DESC, constraint_name, fc.position
      }.freeze

	    # Returns the indexes for the given table. By default, it does not return primary keys.
	    #
      # * <tt>:all</tt> - Returns all indexes, even primary keys.
	    def indexes(table, options={})
	      sql, m = SELECT_INDEXES_SQL, output_identifier_meth
	      table = m[table]
		    ixs = Hash.new{|h,k| h[k] = {:table_name=>table, :columns=>[]}}
		    
		    if options[:all]
	        sql = sql.sub /ORDER BY /, %q{ AND NOT EXISTS (
	SELECT uc.index_name FROM all_constraints uc
	WHERE uc.index_name = i.index_name AND uc.owner = i.owner AND uc.constraint_type = 'P'
)
ORDER BY }
		    end

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
	        pk[:rely] = r.delete(:rely)=='RELY'
	        pk[:enabled] = r.delete(:status)=='ENABLED'
	        pk[:validated] = r.delete(:validated)=='VALIDATED'
	        pk[:columns] << r.delete(:column_name)
	        pk.update r
	      end

	      unless options[:all] or (pks.length>1 and pks[0][:enabled] != pks[1][:enabled])
		      return pks.first
		    end
	      pks
	    end

	    # Returns the foreign keys defined on the given table, as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def foreign_keys(table, options={})
	      sql, m = SELECT_FOREIGN_KEYS_SQL, output_identifier_meth
	      table, fks = m[table], []
        fkh = Hash.new{|h,k| fks.push(h[k]=v={:table_name=>table, :columns=>[]}); v }

	      unless (z = options[:enabled]).nil?
	        sql = sql.sub /WHERE /, "WHERE f.status = #{z ? 'ENABLED' : 'DISABLED'}"
	      end

	      metadata_dataset.with_sql(sql, table.to_s.upcase).each do |r|
	        r = Hash[ r.map{|k,v| [k, (k==:status || v.nil? || v=='') ? v : m[v]]} ]
          fk = fkh[m.call r[:constraint_name]]
	        fk[:rely] = r.delete(:rely)=='RELY'
	        fk[:enabled] = r.delete(:status)=='ENABLED'
	        fk[:validated] = r.delete(:validated)=='VALIDATED'
	        fk[:columns] << r.delete(:column_name)
	        fk.update r
	      end

	      fks
	    end
	    
	    # Returns foreign keys that refer to the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def references(qualified_table, options={})
	    	ds, result    = metadata_dataset, []
	    	schema, table = ds.schema_and_table(qualified_table)
	    	x_cons        = schema.nil? ? 'user_cons' : 'all_cons'
	    	inm           = ds.identifier_input_method
	      hash          = Hash.new{|h,k| result.push(:constraint_name=>ds.send(:output_identifier,k), :columns=>[]); h[k] = result.last }
	    	
	    	ds = ds.select(:f__constraint_name, :f__table_name, :f__rely, :f__status, :f__validated, :fc__column_name).
				        from(:"#{x_cons}traints___f").
				        join(:"#{x_cons}_columns___fc", [[:owner,:owner], [:constraint_name,:constraint_name]]).
				        join(:"#{x_cons}traints___t", [[:owner,:f__r_owner], [:constraint_name,:f__r_constraint_name]]).
				        where(:f__constraint_type=>'R', :t__constraint_type=>'P', :t__table_name=>table.to_s.send(inm))
				ds = ds.where(:t__owner=>schema.to_s.send(inm)) unless schema.nil?
        ds.order(:table_name, :constraint_name, :fc__position).each do |row|
        	hash[row[:constraint_name]].tap do |ref|
	        	ref[:table_name]||= ds.send(:output_identifier,row.delete(:table_name))
	        	ref[:columns]   <<  ds.send(:output_identifier,row.delete(:column_name))
	        	ref[:rely]      ||= row.delete(:rely)=='RELY'
	        	ref[:enabled]   ||= row.delete(:status)=='ENABLED'
	        	ref[:validated] ||= row.delete(:validated)=='VALIDATED'
	        end
        end
        result
	    end
    end
  end
end
