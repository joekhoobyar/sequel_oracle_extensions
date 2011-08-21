require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The oracle_schemata extension adds some schema related methods to the Oracle database adapater.
module Sequel
  module Oracle
    module DatabaseMethods
      
      # Return a hash containing index information for the table. Hash keys are index name symbols
      # and values are subhashes.  The superclass method specifies only two keys :columns and :unique.
      # This implementation provides additional keys in the subhash that expose Oracle-specific index attributes.
      #
			# By default, this method does not return the primary key index.
      # Options:
      # :valid :: Filter by status:  true => only VALID indexes, false => only UNUSABLE indexes
      # :all :: Return all indexes, including the primary key index.
      #
      # Example(s):
      #
      #   DB.indexes(:people)
      #   # { :person_gender=>{
      #   #     :unique=>false,
      #   #     :valid=>true,
      #   #     :db_type=>'BITMAP',
      #   #     :tablespace=>:users,
      #   #     :partitioned=>false,
      #   #     :visible=>true,
      #   #     :compression=>false,
      #   #     :columns=>[:gender]
      #   #   },
      #   #   :person_name=>{
      #   #     :unique=>false,
      #   #     :valid=>true,
      #   #     :db_type=>'NORMAL',
      #   #     :tablespace=>:users,
      #   #     :partitioned=>false,
      #   #     :visible=>true,
      #   #     :compression=>false,
      #   #     :columns=>[:last_name, :first_name]
      #   # } }
      #
      #   # NOTE: Passing :all=>true so we can get the primary key index.
      #   DB.indexes(:employees, :all=>true)
      #   # { :employee_pk=>{
      #   #     :unique=>true,
      #   #     :valid=>true,
      #   #     :db_type=>'NORMAL',
      #   #     :tablespace=>:users,
      #   #     :partitioned=>false,
      #   #     :visible=>true,
      #   #     :compression=>false,
      #   #     :columns=>[:id]
      #   #   },
      #   #   :employee_dept=>{
      #   #     :unique=>false,
      #   #     :valid=>true,
      #   #     :db_type=>'BITMAP JOIN',
      #   #     :tablespace=>:users,
      #   #     :partitioned=>false,
      #   #     :visible=>true,
      #   #     :compression=>false,
      #   #     :columns=>[:dept_id]
      #   # } }
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
							:db_type=>"#{row[:index_type]}#{' JOIN' if row[:join_index]=='YES'}",
							:tablespace=>:"#{outm[row[:tablespace_name]]}", :partitioned=>(row[:partitioned]=='YES'),
							:visible=>(row[:visibility]=='VISIBLE'), :compression=>(row[:compression]!='DISABLED')
						}
					end
					subhash[:columns] << :"#{outm[row[:column_name]]}"
				end
				hash
	    end

	    # Returns a hash containing primary key information for the table, or nil if the table has no primary key.
      # Options:
      # :enabled :: Filter by status: true => only ENABLED primary key, false => only DISABLED primary key
      # :validated :: Filter by validation: true => only VALIDATED primary key, false => only NOT VALIDATED primary key
		  #
		  # Example:
		  #
		  #   DB.primary_key(:people)
		  #   # { :person_id=>{
		  #   #     :rely=>false,
		  #   #     :enabled=>true,
		  #   #     :validated=>true,
		  #   #     :using_index=>:person_pk,
		  #   #     :columns=>[:id]
		  #   # } }
	    def primary_key(table, options={})
	    	result = table_constraints table, 'P', options
				return unless result and not result.empty?
				result.values.first.tap{|pk| pk[:name] = result.keys.first }
	    end

		  # Return a hash containing unique constraint information for the table. Hash keys are constraint name symbols
		  # and values are subhashes. Primary key constraints are _not_ returned by this method.
		  # Options:
		  # :enabled :: Filter by status: true => only ENABLED unique keys, false => only DISABLED unique keys
		  # :validated :: Filter by validation: true => only VALIDATED unique keys, false => only NOT VALIDATED unique keys
		  #
		  # Example:
		  #
		  #   DB.unique_keys(:people)
		  #   # { :person_ssn=>{
		  #   #     :rely=>false,
		  #   #     :enabled=>true,
		  #   #     :validated=>true,
		  #   #     :using_index=>:person_ssn_index,
	    #   #     :columns=>[:ssn]
		  #   #   },
		  #   #   :person_dlnum=>{
		  #   #     :rely=>true,
		  #   #     :enabled=>false,
		  #   #     :validated=>false,
		  #   #     :using_index=>nil,
	    #   #     :columns=>[:drivers_license_state, :drivers_license_number]
		  #   # } }
	    def unique_keys(table, options={})
	    	table_constraints table, 'U', options
	    end
	    
		  # Return a hash containing foreign key information for the table. Hash keys are constraint name symbols
		  # and values are subhashes.
		  # Options:
		  # :enabled :: Filter by status: true => only ENABLED foreign keys, false => only DISABLED foreign keys
		  # :validated :: Filter by validation: true => only VALIDATED foreign keys, false => only NOT VALIDATED foreign keys
		  #
		  # Example:
		  #
		  #   DB.foreign_keys(:employees)
		  #   # { :employee_manager_fk=>{
		  #   #     :rely=>false,
		  #   #     :enabled=>true,
		  #   #     :validated=>true,
		  #   #     :columns=>[:manager_id],
	    #   #     :ref_constraint=>:manager_pk,
	    #   #     :ref_table=>:managers
		  #   #   },
		  #   #   :employee_department_fk=>{
		  #   #     :rely=>false,
		  #   #     :enabled=>true,
		  #   #     :validated=>true,
		  #   #     :columns=>[:department_id],
	    #   #     :ref_constraint=>:department_pk,
	    #   #     :ref_table=>:departments
		  #   # } }
	    def foreign_keys(table, options={})
	    	table_constraints table, 'R', options
	    end
	    
		  # Return a hash containing foreign key information for keys that _refer_ to this table.  Hash keys are constraint name symbols
		  # and values are subhashes.  Foreign keys for this table are _not_ returned by this method (unless they are self-referential).
		  # Options:
		  # :enabled :: Filter by status: true => only ENABLED foreign keys, false => only DISABLED foreign keys
		  # :validated :: Filter by validation: true => only VALIDATED foreign keys, false => only NOT VALIDATED foreign keys
		  #
		  # Example:
		  #
		  #   DB.references(:employees)
		  #   # { :assignment_employee_fk=>{
		  #   #     :rely=>false,
		  #   #     :enabled=>true,
		  #   #     :validated=>true,
		  #   #     :columns=>[:employee_id],
		  #   #     :ref_constraint=>:employee_pk,
		  #   #     :table=>:assignments
		  #   #   },
		  #   #   :bonus_recipient_fk=>{
		  #   #     :rely=>false,
		  #   #     :enabled=>true,
		  #   #     :validated=>true,
		  #   #     :columns=>[:recipient_id],
		  #   #     :ref_constraint=>:employee_pk,
		  #   #     :table=>:bonuses
		  #   # } }
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
