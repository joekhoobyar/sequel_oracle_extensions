require 'sequel'

# The merge extension adds support for Oracle's MERGE statement.
module Sequel
	class Dataset
    MERGE_CLAUSE_METHODS = clause_methods(:merge, %w'target source join update delete insert')
    
    def merge(&block)
      execute_dui merge_sql(&block)
    end
    
    def merge_using(*values, &block)
      execute_dui merge_using_sql(*values, &block)
    end

    def merge_into(*values, &block)
      execute_dui merge_into_sql(*values, &block)
    end

	  def merge_sql(*values, &block)
      ms = clone
	    ms.opts = { :into=>values.shift, :using=>values.shift, :on=>[values.shift].compact }
			[:update, :insert, :delete].each{|k| ms.opts[k] = values.shift }
	    ms.opts.update :defaults=>@opts[:defaults], :overrides=>@opts[:overrides]
        
      if block_given?
	      ms.extend(MergeBlockCopy)
	      ms.instance_eval(&block)
	      ms = ms.clone(ms.opts)
      end

      ms.opts[:into] ||= @opts[:from].first
      ms.opts[:on] << @opts[:where] if @opts[:where]

      ms.send :_merge_sql
    end
    
    def merge_using_sql(using, *values, &block)
      merge_sql @opts[:from].first, using, *values, &block
    end
    
    def merge_into_sql(into, on, *values, &block)
      merge_sql into, self, on, *values, &block
    end
    
  protected
		
		# SQL fragment specifying the target to merge INTO
		def merge_target_sql(sql)
		  sql << " INTO #{table_ref(@opts[:into])}"
		end
		
		# SQL fragment specifying the source to merge USING
		def merge_source_sql(sql)
		  sql << "\nUSING #{table_ref(@opts[:using])}"
		end
  
	  # SQL fragment specifying what to perform the merge ON
	  def merge_join_sql(sql)
	    sql << "\nON #{literal(@opts[:on])}"
	  end

		# SQL fragment specifying which target rows to DELETE
		def merge_delete_sql(sql)
		  sql << "\nDELETE WHERE #{literal(@opts[:delete])}\n" if @opts[:delete]
		end
		  
		# The SQL fragment specifying the columns and values to UPDATE
		def merge_update_sql(sql)
		  return if not (values = @opts[:update]) or values.empty?
			values = Hash[values] if Array===values and values.all?{|v| Array===v && v.size==2}
		  if Hash === values
		    values = @opts[:defaults].merge(values) if @opts[:defaults]
		    values = values.merge(@opts[:overrides]) if @opts[:overrides]
		    # get values from hash
		    values = values.map do |k, v|
		      "#{k.is_a?(String) && !k.is_a?(LiteralString) ? quote_identifier(k) : literal(k)} = #{literal(v)}"
		    end.join(COMMA_SEPARATOR)
		  end
		  sql << "\nWHEN MATCHED THEN\nUPDATE SET #{values}"
		end
		
		# The SQL fragment specifying the columns and values to INSERT
		def merge_insert_sql(sql)
		  return if not @opts[:insert] or @opts[:insert].empty?
		  columns, values = [], []
		  @opts[:insert].each do |k,v|
		    columns.push(k.is_a?(String) && !k.is_a?(LiteralString) ? quote_identifier(k) : literal(k))
		    values.push(v.is_a?(String) && !v.is_a?(LiteralString) ? quote_identifier(v) : literal(v))
		  end
		  sql << "\nWHEN NOT MATCHED THEN\nINSERT (#{columns.join(COMMA_SEPARATOR)})"
      sql << "\nVALUES (#{values.join(COMMA_SEPARATOR)})"
	  end

	  # The order of methods to call on the MERGE SQL statement
	  def merge_clause_methods
	    MERGE_CLAUSE_METHODS
	  end
	  
	  def _merge_sql
      _merge_alias_tables
      @opts[:on] = @opts[:on].inject(nil) do |a,b|
        b = _merge_expressions b
        b = filter_expr((Array===b && b.size==1) ? b.first : b)
        a ? SQL::BooleanExpression.new(:AND, a, b) : b
      end
      @opts[:insert] = @opts[:defaults].merge(@opts[:insert]) if @opts[:defaults]
      @opts[:insert] = @opts[:insert].merge(@opts[:overrides]) if @opts[:overrides]
      [:insert, :update, :delete].each do |k|
        @opts[k] = _merge_expressions @opts[k], k!=:delete || nil
      end

	    clause_sql(:merge)
	  end
	  
	  # Utility method to create and/or apply table aliaseses for expressions.
	  def _merge_expressions(expr,apply_aliases=nil)
	    if Symbol===expr
		    expr = {expr => expr}
	    elsif expr.is_a?(Array) and not expr.empty? and expr.all?{|x| x.is_a?(Symbol)}
	      expr = expr.inject({}){|h,k| h[k]=k; h}
			elsif expr.nil? or LiteralString===expr
			  return expr
	    end
	    apply_aliases = Sequel.condition_specifier?(expr) if apply_aliases.nil?
	    apply_aliases ? _merge_column_pairs(expr) : expr
	  end
	
	  # Utility method to create any necessary table aliases.
	  def _merge_alias_tables
	    alias_num = @opts[:num_dataset_sources]||0
	    [:into, :using].each do |k|
		    if Symbol===@opts[k]
		      u_table, u_column, u_alias = split_symbol(@opts[k])
		      @opts[k] = "#{@opts[k]}___#{dataset_alias(alias_num += 1)}".to_sym unless u_alias
		    else
		      @opts[k] = @opts[k].as dataset_alias(alias_num += 1) unless @opts[k].respond_to? :aliaz
		    end
	    end
	  end

	private
	
	  # Utility method to qualify column pairs to the target and source table aliases.
	  def _merge_table_aliases
	    @opts.values_at(:into, :using).map{|t| Symbol===t ? split_symbol(t).last : t.aliaz.to_s}
	  end
	
	  # Utility method to qualify column pairs to the target and source table aliases.
	  def _merge_column_pairs(pairs)
	    t1, t2 = _merge_table_aliases
	    merged = pairs.collect do |k, v|
	      k = qualified_column_name(k, t1) if k.is_a?(Symbol)
	      v = qualified_column_name(v, t2) if v.is_a?(Symbol)
	      [k,v]
	    end
	    merged = Hash[*merged.flatten] if Hash === pairs
	    merged
	  end

    # Module used by Dataset#merge that has the effect of making all
    # dataset methods into !-style methods that modify the receiver.
    module MergeBlockCopy
      def each; raise Error, "each cannot be invoked inside a merge block." end
      def into(t) @opts[:into] = t end
      def using(t) @opts[:using] = t end

      %w'on update delete'.each do |m|
	      module_eval <<-eodef
	        def #{m}(*args, &block)
	          @opts[:#{m}] = if block; then Sequel.virtual_row(&block)
	            elsif Hash===args.first; then args.first
		          else args
		          end
	        end
	      eodef
	    end

	    def insert(*args, &block)
	      args = [ args ] unless args.empty?
	      args.push([Sequel.virtual_row(&block)]) if block
	      @opts[:insert] = args.inject([]) do |r,a|
	        if Hash === a.first
		        raise Error, "Invalid insert arguments" unless a.size == 1
	          r.concat a.first.to_a
	        elsif a.size == 2
	          raise Error, "Invalid insert arguments" unless a.all?{|v| Array===v && v.size==2}
		        a.first.each_with_index{|k,i| r.push([k,a.last[i]]) }
		        r
			    else
	          raise Error, "Invalid insert arguments"
	        end
		    end
	    end

      # Merge the given options into the receiver's options and return the receiver
      # instead of cloning the receiver.
      def clone(opts = nil)
        @opts.merge!(opts)
        self
      end
    end
	end
end
