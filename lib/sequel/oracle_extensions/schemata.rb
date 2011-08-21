require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The oracle_schemata extension adds some schema related methods to the Oracle database adapater.
module Sequel
  module Oracle
    module DatabaseMethods
      
      # Return a hash containing index information for the table. Hash keys are index name symbols
      # and values are subhashes.  The superclass method specifies only two keys :columns and :unique.
      # This extension provides additional keys in the subhash that expose Oracle-specific index attributes.
      #
			# By default, this method does not return the primary key index.
	    #
      # * <tt>:valid</tt> - Only look for indexes that are valid (true) or unusable (false). By default (nil),
      #   looks for any matching index.
      # * <tt>:all</tt> - Returns all indexes, even ones used for primary keys.
      #
	    def indexes(table, opts={})
	    	ds, result    = metadata_dataset, []
				outm          = lambda{|k| ds.send :output_identifier, k}
	    	schema, table = ds.schema_and_table(table).map{|k| k.to_s.send(ds.identifier_input_method) if k} 
	    	
	    	# Build the dataset and apply filters for introspection of indexes.
	    	ds = ds.select(:i__index_name, :i__index_type, :i__join_index, :i__partitioned, :i__status,
	    	               :i__uniqueness, :i__visibility, :i__compression, :i__tablespace_name, :ic__column_name).
				        from(:"all_indexes___i").
				        join(:"all_ind_columns___ic", [ [:index_owner,:owner], [:index_name,:index_name] ]).
								where(:i__table_name=>table, :i__dropped=>'NO').
	              order(:status.desc, :index_name, :ic__column_position)
				ds = ds.where :i__owner => schema unless schema.nil?
				ds = ds.where :i__status => (opts[:valid] ? 'VALID' : 'UNUSABLE') unless opts[:valid].nil?
				unless opts[:all]
				  pk = from(:all_constraints.as(:c)).where(:c__constraint_type=>'P').
					     where(:c__index_name=>:i__index_name, :c__owner=>:i__owner)
					ds = ds.where ~pk.exists
				end

				# Return the indexes as a hash of subhashes, including a column list.
				hash = {}
				ds.each do |row|
					key = :"#{outm[row[:index_name]]}"
					unless subhash = hash[key]
						subhash = hash[key] = {
							:columns=>[], :unique=>(row[:uniqueness]=='UNIQUE'), :valid=>(row[:status]=='VALID'),
							:db_type=>row[:index_type], :tablespace=>:"#{outm[row[:tablespace_name]]}",
							:join_index=>(row[:join_index]=='YES'), :partitioned=>(row[:partitioned]=='YES'),
							:visible=>(row[:visibility]=='VISIBLE'), :compression=>(row[:compression]!='DISABLED')
						}
					end
					subhash[:columns] << :"#{outm[row[:column_name]]}"
				end
				hash
	    end

	    # Returns the primary key for the given +table+ (or +schema.table+), as a hash.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for any matching key.
      #
	    def primary_key(table, options={})
	    	result = table_constraints table, 'P', options
				return unless result and not result.empty?
				result.values.first.tap{|pk| pk[:name] = result.keys.first }
	    end

	    # Returns unique constraints defined on the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def unique_keys(table, options={})
	    	table_constraints table, 'U', options
	    end
	    
	    # Returns foreign keys defined on the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def foreign_keys(table, options={})
	    	table_constraints table, 'R', options
	    end
	    
	    # Returns foreign keys that refer to the given +table+ (or +schema.table+), as an array of hashes.
	    #
      # * <tt>:enabled</tt> - Only look for keys that are enabled (true) or disabled (false). By default (nil),
      #   looks for all matching keys.
	    def references(table, options={})
	    	table_constraints table, 'R', options.merge(:table_name_column=>:t__table_name)
	    end
	    
	  private
	  	
	  	# Internal helper method for introspection of table constraints.
	  	def table_constraints(table, constraint_type, options={})
	    	ds, result    = metadata_dataset, []
				outm          = lambda{|k| ds.send :output_identifier, k}
	    	schema, table = ds.schema_and_table(table).map{|k| k.to_s.send(ds.identifier_input_method) if k} 
	    	x_cons        = schema.nil? ? 'user_cons' : 'all_cons'
	    	
	    	# Build the dataset and apply filters for introspection of constraints.
				# Also allows the caller to customize the dataset.
	    	ds = ds.select(:c__constraint_name, :c__table_name, :c__rely, :c__status, :c__validated, :cc__column_name).
				        from(:"#{x_cons}traints___c").
				        join(:"#{x_cons}_columns___cc", [ [:owner,:owner], [:constraint_name,:constraint_name] ]).
								where((options[:table_name_column]||:c__table_name)=>table, :c__constraint_type=>constraint_type).
	              order(:table_name, :status.desc, :constraint_name, :cc__position)
				ds = ds.where :c__owner => schema unless schema.nil?
				ds = ds.where :c__status => (options[:enabled] ? 'ENABLED' : 'DISABLED') unless options[:enabled].nil?
				if constraint_type == 'R'
	        ds = ds.select_more(:c__r_constraint_name, :t__table_name.as(:r_table_name)).
					        join(:"#{x_cons}traints___t", [ [:owner,:c__r_owner], [:constraint_name,:c__r_constraint_name] ]).
	                where(:t__constraint_type=>'P')
				else
	        ds = ds.select_more(:c__index_name)
				end
				ds = ds.limit(1) if constraint_type == 'P'
				
				# Return the table constraints as a hash of subhashes, including a column list.
				hash = {}
				ds.each do |row|
					key = :"#{outm[row[:constraint_name]]}"
					unless subhash = hash[key]
						subhash = hash[key] = {
							:rely=>(row[:rely]=='RELY'), :enabled=>(row[:status]=='ENABLED'),
							:validated=>(row[:validated]=='VALIDATED'), :columns=>[]
						}
						if row.include? :r_constraint_name
							subhash[:ref_constraint] = :"#{outm[row[:r_constraint_name]]}"
							if options[:table_name_column]==:t__table_name
							then subhash[:table] = :"#{outm[row[:table_name]]}"
							else subhash[:ref_table] = :"#{outm[row[:r_table_name]]}"
							end
						elsif row.include? :index_name
							subhash[:using_index] = :"#{outm[row[:index_name]]}"
						end
					end
					subhash[:columns] << :"#{outm[row[:column_name]]}"
				end
				hash
	  	end
    end
  end
end
