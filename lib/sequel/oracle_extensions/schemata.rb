require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The oracle_schemata extension adds some schema related methods to the Oracle database adapater.
module Sequel
  module Oracle
    module DatabaseMethods
      
      # Specifies flag attributes which are considered implicit and, thus, do not require DDL clauses.
      IMPLICIT_FLAG_ATTRIBUTES = {
        :parallel=>false, :compress=>false, :logging=>true, :visible=>true,
        :rely=>false, :enabled=>true, :validate=>true
      }
      
      # Returns a hash containing expanded table metadata that exposes Oracle-specific table attributes.
      #
      # Basic Attributes:
      # :columns :: a columns subhash derived from a call to the #schema(table,options={}) method
      # :schema_name :: the name of the schema that owns this table
      #
      # Extended Attributes: (NOTE: some of the following attributes may be nil with older OCI clients)
      # :index_only :: is this an index-organized table?
      # :clustered :: is this a clustered table?
      # :partitioned :: is this a partitioned table?
      # :temporary :: is this a global temporary table?
      # :typed :: is this a ... typed table?  ( not sure what that means :-/ )
      #
      def table_metadata(table,options={})
		    columns    = schema table, options
        attributes = columns.instance_eval{ remove_instance_variable :@features }
		    attributes[:columns] = columns
		    
		    # Collect table partitioning information, if applicable.
		    if attributes[:partitioning]
		    	ds, result    = metadata_dataset, []
					outm          = sql_ident_to_sym_proc ds
	    	  schema, table = ds.schema_and_table(table).map{|k| k.to_s.send(ds.identifier_input_method) if k} 
          who           = schema.nil? ? 'user' : 'all'
					ds = ds.where :owner => schema unless schema.nil?
					
					# Basic partitioning info.
					attributes[:partitioning] = Hash[
						ds.select(:partitioning_type.as(:type), :interval, :subpartitioning_type.as(:subtype)).
		    	    from(:"#{who}_part_tables").first(:table_name=>table).
		    	    map{|k,v| [k, k==:interval ? v : v.downcase.to_sym] }
			   	]
		    	
		    	# Partitioning key column info. 
					attributes[:partitioning][:key] =
						ds.select(:column_name).from(:"#{who}_part_key_columns").order(:column_position).
		    	    where(:object_type=>'TABLE', :name=>table).map{|r| outm[r.values.first] }
				end
	    	    
		    attributes
      end
      
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
      #   #     :compress=>false,
      #   #     :columns=>[:gender]
      #   #   },
      #   #   :person_name=>{
      #   #     :unique=>false,
      #   #     :valid=>true,
      #   #     :db_type=>'NORMAL',
      #   #     :tablespace=>:users,
      #   #     :partitioned=>false,
      #   #     :visible=>true,
      #   #     :compress=>false,
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
      #   #     :compress=>false,
      #   #     :columns=>[:id]
      #   #   },
      #   #   :employee_dept=>{
      #   #     :unique=>false,
      #   #     :valid=>true,
      #   #     :db_type=>'BITMAP',
      #   #     :type=>:bitmap,
      #   #     :join=>[:dept_id],
      #   #     :tablespace=>:users,
      #   #     :partitioned=>false,
      #   #     :visible=>true,
      #   #     :compress=>false,
      #   #     :columns=>[:departments__id]
      #   # } }
	    def indexes(table, opts={})
	    	ds, result    = metadata_dataset, []
				outm          = sql_ident_to_sym_proc ds
	    	schema, table = ds.schema_and_table(table).map{|k| k.to_s.send(ds.identifier_input_method) if k} 
	    	who           = schema.nil? ? 'user' : 'all'
	    	
	    	# Build the dataset and apply filters for introspection of indexes.
	    	ds = ds.select(:i__index_name, :i__index_type, :i__join_index, :i__partitioned, :i__status,
	    	               :i__uniqueness, :i__visibility, :i__compression, :i__tablespace_name,
	    	               :i__logging, :i__degree, :i__instances, :ic__column_name).
				        from(:"#{who}_indexes___i").
				        join(:"#{who}_ind_columns___ic", [ [:index_name,:index_name] ]).
								where(:i__table_name=>table, :i__dropped=>'NO').
	              order(:status.desc, :index_name, :ic__column_position)
				ds = ds.where :i__owner => schema, :c__index_owner => schema  unless schema.nil?
				ds = ds.where :i__status => (opts[:valid] ? 'VALID' : 'UNUSABLE') unless opts[:valid].nil?
				unless opts[:all]
				  pk = from(:"#{who}_constraints".as(:c)).where(:c__constraint_type=>'P').
					     where(:c__index_name => :i__index_name)
					pk = pk.where :c__owner => schema unless schema.nil?
					ds = ds.where ~pk.exists
				end

				# Collect the indexes as a hash of subhashes, including a column list.
				# As a followup, collect any additional metadata about the indexes (such as bitmap join columns).
				hash, join_indexes = {}, []
				ds.each do |row|
					key = outm[row[:index_name]]
					unless subhash = hash[key]
						subhash = hash[key] = {
							:columns=>[], :unique=>(row[:uniqueness]=='UNIQUE'),
							:db_type=>row[:index_type], :logging=>(row[:logging]=='YES'),
							:parallel=>(row[:degree]!='1' || row[:instances]!='1'),
							:tablespace=>outm[row[:tablespace_name]], :partitioned=>(row[:partitioned]=='YES'),
							:visible=>(row[:visibility]=='VISIBLE'), :compress=>(row[:compression]!='DISABLED')
						}
						case subhash[:db_type]
            when /\b(BITMAP|NORMAL|DOMAIN)$/
						  subhash[:type] = $1.downcase.intern
						end
            subhash[:function_based] = true if /^FUNCTION-BASED\b/ === subhash[:db_type]
            subhash[:valid] = (row[:status]=='VALID') unless row[:status]=='N/A'
				    if row[:join_index]=='YES'
						  join_indexes << row[:index_name]
						  subhash[:join] = []
				    end
					end
					subhash[:columns] << outm[row[:column_name]]
				end
				ds = metadata_dataset.from(:"#{who}_join_ind_columns").where(:index_name=>join_indexes)
				ds = ds.where :index_owner => schema unless schema.nil?
				ds.each do |row|
					subhash    = hash[outm[row[:index_name]]]
					ref_column = outm[row[:outer_table_column]]
					pos        = subhash[:columns].index ref_column
					subhash[:columns][pos] = outm["#{row[:outer_table_name]}__#{ref_column}"]
					subhash[:join][pos]    = outm[row[:inner_table_column]]
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

      # Overridden to support various Oracle-specific options.
      def alter_table(name, generator=nil, options=nil, &block)
        if Hash === options
          generator ||= Schema::AlterTableGenerator.new(self, &block)
		      alter_table_sql_list(name, generator.operations, options).
		        flatten.each {|sql| execute_ddl(sql)}
		      remove_cached_schema(name)
		      nil
        else
	        super(name, generator, &block)
        end
      end

    protected

      # Type-safe (and nil safe) conversion for SQL identifers to Ruby symbols.
      # Returns a proc that converts String types and passes through all other types
      def sql_ident_to_sym_proc(dataset)
        lambda do |k|
          unless String===k then k else
            k = dataset.send :output_identifier, k
            String===k ? k.intern : k
          end
        end
      end

    private

      # Overridden because Oracle has slightly different syntax.
      def alter_table_sql(table, op)
	      case op[:op]
        when :add_column
          "ALTER TABLE #{quote_schema_table(table)} ADD #{column_definition_sql(op)}"
        else
          return super(table, op)
        end
	    end
	    
      # Overridden to support various Oracle-specific options.
	    def alter_table_sql_list(table, operations, options=nil)
	      return super(table, operations) unless Hash===options
	      
	      prologue = "ALTER TABLE #{quote_schema_table(table)} "
	      sql      = operations.map do |op|
	        frag = alter_table_sql table, op
	        raise ArgumentError unless frag.slice![0,prologue.length] == prologue
	        frag
	      end
	      sql.push(table_options_sql(options)).join ' '
      end

      # Overridden to support various Oracle-specific options.
      def column_references_sql(options)
        sql = [super(options)]
	      sql << flag_option_sql(options, :rely)
	      sql << flag_option_sql(options, :enable, 'DISABLE')
	      sql << flag_option_sql(options, :validate)
	      sql.join ' '
      end
	
	    # Overridden to support various Oracle-specific options.
	    def create_table_sql(name, generator, options)
        a, b = super(name, generator, options), table_options_sql(options)
        "#{a}\n#{b}"
	    end

		  # Overridden because Oracle has a 30 character maximum identifier length.
		  def default_index_name(table_name, columns)
		    schema, table = schema_and_table(table_name)
		    ds = DB[:all_indexes].where(:table_name=>table,:dropped=>'NO')
		    ds = ds.where :owner=>schema unless schema.nil?
		    "#{table[0,25]}_ix%2.2d" % [ds.count + 1]
		  end
    
	    # SQL DDL statement for creating an index for the table with the given name
	    # and index specifications.
	    def index_definition_sql(table_name, index)
	      raise Error, "Partial indexes are not supported for this database" if index[:where]

	      # Basic index creation DDL.
	      sql = ["CREATE"]
	      case index[:type]
	      when :bitmap
		      raise Error, "Bitmap indexes cannot be unique" if index[:unique]
	        sql << 'BITMAP'
	      when NilClass, :normal
	        sql << 'UNIQUE' if index[:unique]
	      else
	        raise Error, "Index type #{index[:type].inspect} is not supported for this database"
	      end
	      index_name = index[:name] || default_index_name(table_name, index[:columns])
	      qualified_table_name = quote_schema_table table_name
	      sql << "INDEX #{quote_identifier(index_name)} ON #{qualified_table_name}"
	      
	      # Index columns and join indexes.
        index_join, index_columns = *index.values_at(:join,:columns)
	      sql << literal(index_columns)
        if index_join
		      raise Error, "Join clauses are only supported for bitmap indexes" if index[:type]!=:bitmap
		      sql << "FROM #{qualified_table_name},"
		      sql << index_columns.map{|k| quote_identifier schema_and_table(k).first }.uniq.join(', ')
		        
		      # TODO: Document this short-hand syntax:    {:columns=>[:ref_table__ref_column], :join=>[:fk_column]}
          if Array===index_join and index_join.length==index_columns.length and index_join.all?{|k| Symbol===k}
            index_join = Hash[ index_join.map{|k| :"#{table_name}__#{k}" }.zip(index_columns) ]
          end

	        sql << "WHERE #{filter_expr(index_join)}"
	      end
	      
	      # Index attributes and options.
	      sql << 'LOCAL' if index[:partitioned]
	      sql << flag_option_sql(index, :parallel)
	      sql << flag_option_sql(index, :logging)
	      sql << "TABLESPACE #{quote_identifier(index[:tablespace])}" if index[:tablespace]
	      sql << flag_option_sql(index, :visible, 'INVISIBLE')
	      sql << compress_option_sql(index)
	      sql << index[:options] if String === index[:options]
	      sql << 'UNUSABLE' if FalseClass === index[:valid]
	      sql.compact.join ' '
	    end
	    
      # SQL DDL clauses for altering and/or creating tables.
	    def table_options_sql(options)
	      sql = []
	      sql << flag_option_sql(options, :parallel)
	      sql << flag_option_sql(options, :logging)
	      sql << flag_option_sql(options, :monitoring)
	      sql << "TABLESPACE #{quote_identifier(options[:tablespace])}" if options[:tablespace]
	      sql << compress_option_sql(options)
	      sql << options[:options] if String === options[:options]
	      sql.compact.join ' '
	    end
	    
      # SQL DDL clause for specifying on/off flags
	    def flag_option_sql(attrs, key, off="NO#{key}".upcase, on=key.to_s.upcase, implicit=IMPLICIT_FLAG_ATTRIBUTES[key])
	      case attrs[key]
	      when NilClass, implicit
	      when TrueClass then on
	      when FalseClass then off
	      else raise Error, "Unsupported or invalid #{key} option"
	      end
	    end
	    
      # SQL DDL clause for specifying compression in a table or index.
	    def compress_option_sql(attrs)
	      case value=attrs[:compress]
	      when Fixnum, Integer then "COMPRESS(#{value})"
	      else flag_option_sql attrs, :compress
	      end
      end
	      
	  	# Internal helper method for introspection of table constraints.
	  	def table_constraints(table, constraint_type, options={})
	    	ds, result    = metadata_dataset, []
				outm          = sql_ident_to_sym_proc ds
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
					key = outm[row[:constraint_name]]
					unless subhash = hash[key]
						subhash = hash[key] = {
							:rely=>(row[:rely]=='RELY'), :enable=>(row[:status]=='ENABLED'),
							:validate=>(row[:validated]=='VALIDATED'), :columns=>[]
						}
						if row.include? :r_constraint_name
							subhash[:ref_constraint] = outm[row[:r_constraint_name]]
							if options[:table_name_column]==:t__table_name
							then subhash[:table] = outm[row[:table_name]]
							else subhash[:ref_table] = outm[row[:r_table_name]]
							end
						elsif row.include? :index_name
							subhash[:using_index] = outm[row[:index_name]]
						end
					end
					subhash[:columns] << outm[row[:column_name]]
				end
				hash
	  	end
    end

		# Methods that override existing functionality on Sequel::Oracle::Database.
    module DatabaseExtensions

    private
          
      # Implemented in order to override existing functionality on Sequel::Oracle::Database.
      def self.append_features(base)
        instance_methods(false).each do |k|
          base.send :remove_method, k if base.instance_method(k).owner == base
        end
        super
      end

    public
          
      # Overridden to collect additional table-level information from the metadata.
      #
      # See Sequel::Oracle::Database#schema_parse_table for the original implementation.
      def schema_parse_table(table, opts={})
        ds = dataset
        ds.identifier_output_method = :downcase
        schema_and_table = "#{"#{quote_identifier(opts[:schema])}." if opts[:schema]}#{quote_identifier(table)}"
        table_schema = []
        metadata = transaction(opts){|conn| conn.describe_table(schema_and_table)}
        metadata.columns.each do |column|
          table_schema << [
            column.name.downcase.to_sym,
            {
              :type => column.data_type,
              :db_type => column.type_string.split(' ')[0],
              :type_string => column.type_string,
              :charset_form => column.charset_form,
              :char_used => column.char_used?,
              :char_size => column.char_size,
              :data_size => column.data_size,
              :precision => column.precision,
              :scale => column.scale,
              :fsprecision => column.fsprecision,
              :lfprecision => column.lfprecision,
              :allow_null => column.nullable?
            }
          ]
        end
        table_schema.instance_variable_set :@features, {
          :owner         => :"#{metadata.obj_schema.downcase}",
          :clustered     => (metadata.clustered? rescue nil),
          :temporary     => (metadata.is_temporary? rescue nil),
          :partitioning  => (metadata.partitioned? rescue nil),
          :typed         => (metadata.is_typed? rescue nil),
          :index_only    => (metadata.index_only? rescue nil)
        }
        table_schema
      end
    end
  end
end

Sequel.require 'adapters/oracle' unless defined? ::Sequel::Oracle::Database
::Sequel::Oracle::Database.class_eval do
  #remove_method :schema_parse_table
  include ::Sequel::Oracle::DatabaseExtensions
end
