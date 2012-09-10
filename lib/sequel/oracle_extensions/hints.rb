require 'sequel'
Sequel.require 'adapters/shared/oracle'

# The hint extension adds support for Oracle hints
module Sequel

	[Dataset, Oracle::DatasetMethods].each do |t|
	  t.instance_eval do
	    constants.grep(/_CLAUSE_METHODS$/).each do |k|
	      type = k[0,k.length - 15].downcase
	      meth = :"#{type}_hint_sql"
			  unless const_get(k).include? meth
		      list = remove_const(k).dup
		      begin list = [list.shift, meth].concat(list)
		      ensure const_set k, list
		      end
	      end
	    end
	  end
	end

  module Oracle
    module DatasetMethods
      
      def hint(*args) clone(:hints => _hints(*args){|v| v.dup}) end
      def hint!(*args) @opts[:hints] = _hints(*args){|v| v.dup}; self end
      def hints(*args) clone(:hints => _hints(*args){|v| []}) end
      def hints!(*args) @opts[:hints] = _hints(*args){|v| []}; self end
      
      def hint_sql(type, sql)
        if @opts.include? :hints and @opts[:hints].include? type and not @opts[:hints][type].empty?
	        sql << " /*+ #{@opts[:hints][type].join ' '} */"
	      end
      end
      
			%w(select insert update delete merge).map{|k| k.to_sym}.each do |k|
        define_method(:"#{k}_hint") {|*args| hint k, *args}
        define_method(:"#{k}_hint!") {|*args| hint! k, *args}
        define_method(:"#{k}_hints") {|*args| hints k, *args}
        define_method(:"#{k}_hints!") {|*args| hints! k, *args}
        define_method(:"#{k}_hint_sql") {|sql| hint_sql k, sql}
      end
    
    protected

	    def _hints(*args, &block)
        type = args.shift if Symbol === args.first
        hints = hints_copy type, &block
	      if type.nil?
	        args.each do |arg|
	          arg = { :select => arg } unless Hash === arg
		        arg.each{|k,v| hint_list_add hints[k], v}
          end
	      else
		      hint_list_add hints[type], args
        end
        hints
	    end
      
    private
    
	    def hints_copy(type=nil)
	      hints = Hash.new{|h,k| h[k] = []}
        @opts[:hints].each{|k,v| v = yield v if type.nil? or type==k; hints[k] = v} if @opts.include? :hints
	      hints
	    end

	    def hint_list_add(list, hint)
	      case hint
	      when String; list.push hint
	      when Array;  list.concat hint
	      else raise Error, "Invalid SQL hint value '#{hints.class.name}': must be an Array or String"
	      end
	    end
		end
  end
end  
