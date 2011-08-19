require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The oracle_schemata extension adds some schema related methods to the Oracle database adapater.
module Sequel
  module Oracle
    module DatabaseMethods
      
	    # Returns the indexes for the given +table+ (or +schema.table+), as an array of hashes.
			# By default, it does not return primary keys.
	    #
      # * <tt>:valid</tt> - Only look for indexes that are valid (true) or unusable (false). By default (nil),
      #   looks for any matching index.
      # * <tt>:all</tt> - Returns all indexes, even ones used for primary keys.
	    def indexes(qualified_table, options={})
	    	ds, result    = metadata_dataset, []
				outm          = lambda{|k| ds.send :output_identifier, k}
	    	schema, table = ds.schema_and_table(qualified_table).map{|k| k.to_s.send(ds.identifier_input_method) if k} 
	    	
	    	# Build the dataset and apply filters for introspection of indexes.
	    	ds = ds.select(:i__index_name, :i__status, :i__uniqueness, :i__visibility,
				               :i__index_type, :i__join_index, :ic__column_name).
				        from(:"all_indexes___i").
				        join(:"all_ind_columns___ic", [ [:index_owner,:owner], [:index_name,:index_name] ]).
								where(:i__table_name=>table, :i__dropped=>'NO').
	              order(:status.desc, :index_name, :ic__column_position)
				unless schema.nil?
					ds = ds.where :i__owner => schema
				end
				unless (z = options[:valid]).nil?
					ds = ds.where :i__status => (z ? 'VALID' : 'UNUSABLE')
				end
				if options[:all]
				  pk = from(:all_constraints.as(:c)).where(:c__constraint_type=>'P').
					     where(:c__index_name=>:i__index_name, :c__owner=>:i__owner)
					ds = ds.where ~pk.exists
				end

				# Return the table constraints as an array of hashes, including a column list.
	      hash = Hash.new do |h,k|
	      	result.push :index_name=>outm[k], :table_name=>outm[table], :columns=>[]
	      	h[k] = result.last
	      end
        ds.each do |row|
        	ref = hash[row[:index_name]]
					ref[:index_type]        ||= row[:index_type]
					ref[:join_index]        ||= row[:join_index]=='YES'
        	ref[:valid]             ||= row[:status]=='VALID'
        	ref[:uniqueness]        ||= row[:uniqueness]=='UNIQUE'
        	ref[:visibility]        ||= row[:visibility]=='VISIBLE'
        	ref[:columns]           <<  outm[row[:column_name]]
        end
        result
	    end

	    # Returns the primary key for the given +table+ (or +schema.table+), as a hash.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for any matching key.
      # * <tt>:all</tt>     - Return an array of matching keys, instead of the first matching key.
      #
	    def primary_key(qualified_table, options={})
	    	result = table_constraints qualified_table, 'P', options
				options[:all] ? result : result.first
	    end

	    # Returns unique constraints defined on the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def unique_keys(qualified_table, options={})
	    	table_constraints qualified_table, 'U', options
	    end
	    
	    # Returns foreign keys defined on the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def foreign_keys(qualified_table, options={})
	    	table_constraints qualified_table, 'R', options
	    end
	    
	    # Returns foreign keys that refer to the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def references(qualified_table, options={})
	    	table_constraints qualified_table, 'R', options.merge(:table_name_column=>:t__table_name)
	    end
	    
	  private
	  	
	  	# Internal helper method for introspection of table constraints.
	  	def table_constraints(qualified_table, constraint_type, options={})
	    	ds, result    = metadata_dataset, []
				outm          = lambda{|k| ds.send :output_identifier, k}
	    	schema, table = ds.schema_and_table(qualified_table).map{|k| k.to_s.send(ds.identifier_input_method) if k} 
	    	x_cons        = schema.nil? ? 'user_cons' : 'all_cons'
	    	
	    	# Build the dataset and apply filters for introspection of constraints.
				# Also allows the caller to customize the dataset.
	    	ds = ds.select(:c__constraint_name, :c__table_name, :c__rely, :c__status, :c__validated, :cc__column_name).
				        from(:"#{x_cons}traints___c").
				        join(:"#{x_cons}_columns___cc", [ [:owner,:owner], [:constraint_name,:constraint_name] ]).
								where((options[:table_name_column]||:c__table_name)=>table, :c__constraint_type=>constraint_type).
	              order(:table_name, :status.desc, :constraint_name, :cc__position)
				unless schema.nil?
					ds = ds.where :c__owner => schema
				end
				unless (z = options[:enabled]).nil?
					ds = ds.where :c__status => (z ? 'ENABLED' : 'DISABLED')
				end

				if constraint_type == 'R'
	        ds = ds.select_more(:c__r_constraint_name, :t__table_name.as(:r_table_name)).
					        join(:"#{x_cons}traints___t", [ [:owner,:c__r_owner], [:constraint_name,:c__r_constraint_name] ]).
	                where(:t__constraint_type=>'P')
				else
	        ds = ds.select_more(:c__index_name)
				end
				ds = yield ds, table if block_given?
				
				# Return the table constraints as an array of hashes, including a column list.
	      hash = Hash.new do |h,k|
	      	result.push :constraint_name=>outm[k], :constraint_type=>constraint_type, :columns=>[]
	      	h[k] = result.last
	      end
        ds.each do |row|
        	ref = hash[row[:constraint_name]]
        	ref[:table_name]        ||= outm[row[:table_name]]
        	ref[:rely]              ||= row[:rely]=='RELY'
        	ref[:enabled]           ||= row[:status]=='ENABLED'
        	ref[:validated]         ||= row[:validated]=='VALIDATED'
        	ref[:columns]           <<  outm[row[:column_name]]

					if row.include? :r_constraint_name
						ref[:r_constraint_name] ||= outm[row[:r_constraint_name]]
						ref[:r_table_name]      ||= outm[row[:r_table_name]]
					end
					if row[:index_name]
						ref[:index_name]        ||= outm[row[:index_name]]
					end
        end
        result
	  	end
    end
  end
end
