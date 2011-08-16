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

	    # Returns the primary key for the given +table+ (or +schema.table+), as a hash.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for any matching key.
      # * <tt>:all</tt>     - Return an array of matching keys, instead of the first matching key.
      #
	    def primary_key(qualified_table, options={})
	    	result = table_constraints(qualified_table, options) do |ds,table_name,x_cons|
	        ds.where(:c__constraint_type=>'P', :c__table_name=>table_name).
	           order(:status.desc, :index_name, :cc__position)
				end
				options[:all] ? result : result.first
	    end

	    # Returns the foreign keys defined on the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def foreign_keys(qualified_table, options={})
	    	table_constraints(qualified_table, options) do |ds,table_name,x_cons|
	        ds.where(:c__constraint_type=>'R', :c__table_name=>table_name).
	           order(:table_name, :constraint_name, :cc__position)
				end
	    end
	    
	    # Returns foreign keys that refer to the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def references(qualified_table, options={})
	    	table_constraints(qualified_table, options) do |ds,table_name,x_cons|
	        ds.join(:"#{x_cons}traints___t", [[:owner,:c__r_owner], [:constraint_name,:c__r_constraint_name]]).
	           where(:c__constraint_type=>'R', :t__constraint_type=>'P', :t__table_name=>table_name).
	           order(:table_name, :constraint_name, :cc__position)
				end
	    end
	    
	  private
	  	
	  	# Internal helper method for introspection of table constraints.
	  	def table_constraints(qualified_table,options={})
	    	ds, result    = metadata_dataset, []
	    	schema, table = ds.schema_and_table(qualified_table)
	    	x_cons        = schema.nil? ? 'user_cons' : 'all_cons'
	    	inm           = ds.identifier_input_method
	    	
	    	# Build the dataset and apply filters for introspection of constraints.
				# Also allows the caller to customize the dataset.
	    	ds = ds.select(:c__constraint_name, :c__table_name, :c__rely, :c__status, :c__validated,
	    	               :cc__column_name, :c__index_name, :c__constraint_type).
				        from(:"#{x_cons}traints___c").
				        join(:"#{x_cons}_columns___cc", [[:owner,:owner], [:constraint_name,:constraint_name]])
				unless schema.nil?
					ds = ds.where :c__owner => schema.to_s.send(inm)
				end
				unless (z = options[:enabled]).nil?
					ds = ds.where :status => (z ? 'ENABLED' : 'DISABLED')
				end
				ds = yield ds, table.to_s.send(inm), x_cons
				
				# Return the table constraints as an array of hashes, including a column list.
	      hash = Hash.new do |h,k|
	      	result.push :constraint_name=>ds.send(:output_identifier,k), :columns=>[]
	      	h[k] = result.last
	      end
        ds.each do |row|
        	ref = hash[row[:constraint_name]]
        	ref[:table_name]||= ds.send(:output_identifier,row.delete(:table_name))
        	ref[:columns]   <<  ds.send(:output_identifier,row.delete(:column_name))
        	ref[:rely]      ||= row.delete(:rely)=='RELY'
        	ref[:enabled]   ||= row.delete(:status)=='ENABLED'
        	ref[:validated] ||= row.delete(:validated)=='VALIDATED'
        	ref[:index_name]||= ds.send(:output_identifier,row.delete(:index_name)) if row[:index_name]
        end
        result
	  	end
    end
  end
end
