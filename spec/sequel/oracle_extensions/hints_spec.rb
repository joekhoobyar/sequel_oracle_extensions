require File.expand_path('../../../spec_helper', __FILE__)
require 'sequel/oracle_extensions/merge'
require 'sequel/oracle_extensions/hints'

describe "Sequel::OracleExtensions::Hints" do

  CLAUSES = %w(SELECT INSERT UPDATE DELETE MERGE)
  TYPES = CLAUSES.map{|clause| clause.downcase.intern}

  before(:all) do
    @db = Sequel.connect(DATABASE_URL)
  end

	def apply_hints!(*args)
	  @old_hints = @ds.opts[:hints]
	  @new_ds    = @ds.__send__(@method, *args)
	
	  @new_ds.should be_kind_of(Sequel::Dataset)
	  @new_ds.opts[:hints].should_not be_empty
	end
  
  it "hooks into dataset clause methods" do
    [Sequel::Dataset, Sequel::Oracle::DatasetMethods].each do |klass|
	    CLAUSES.each do |clause|
	      next unless klass.const_defined?(k = :"#{clause}_CLAUSE_METHODS")
		    klass.const_get(k).first.should == "#{clause.downcase}_hint_sql".intern
		  end
	  end
  end

  share_examples_for "dataset modifying" do
    after(:each) do
		  @ds.should equal(@new_ds)
		  @ds.opts[:hints].should_not == @old_hints
    end
  end
  
  share_examples_for "dataset cloning" do
    after(:each) do
		  @ds.should_not equal(@new_ds)
		  @ds.opts[:hints].should == @old_hints
    end
  end
  
  share_examples_for "standard callspec" do
	  it "callspec (String) applies :select hints" do
	    apply_hints! @hints.first
      hints_to_check(@new_ds, :select, @hints[0,1]).should == @hints[0,1]
	  end
	  it "callspec (String, ...) applies :select hints" do
	    apply_hints! *@hints
      hints_to_check(@new_ds, :select, @hints).should == @hints
	  end
	  it "callspec (clause, String) applies clause hints" do
	    TYPES.each do |type|
        apply_hints! type, @hints.first
	      hints_to_check(@new_ds, type, @hints[0,1]).should == @hints[0,1]
	    end
	  end
	  it "callspec (clause, String, ...) applies clause hints" do
	    TYPES.each do |type|
        apply_hints! type, *@hints
	      hints_to_check(@new_ds, type, @hints).should == @hints
	    end
	  end
  end

  share_examples_for "clause-specific callspec" do
	  it "callspec (String) applies hints" do
	    apply_hints! @hints.first
      hints_to_check(@new_ds, @clause, @hints[0,1]).should == @hints[0,1]
	  end
	  it "callspec (String, ...) applies hints" do
	    apply_hints! *@hints
      hints_to_check(@new_ds, @clause, @hints).should == @hints
	  end
  end
  
  describe "hints" do
    before(:each){ @ds, @hints = @db[:dual], ['foo', 'bar'] }
    
    COMMON_GROUP_BODY = Proc.new do |group,name|
      group.class_eval do
			  describe "##{name}" do
		      before(:each){ @method = :"#{name}" }
			    it_should_behave_like "dataset cloning"
			    it_should_behave_like "standard callspec"
			  end
			  describe "##{name}!" do
		      before(:each){ @method = :"#{name}!" }
			    it_should_behave_like "dataset modifying"
			    it_should_behave_like "standard callspec"
			  end
			  TYPES.each do |clause|
				  describe "##{clause}_#{name}" do
			      before(:each){ @clause, @method = clause, :"#{clause}_#{name}"}
				    it_should_behave_like "dataset cloning"
				    it_should_behave_like "clause-specific callspec"
				  end
				  describe "##{clause}_#{name}!" do
			      before(:each){ @clause, @method = clause, :"#{clause}_#{name}!"}
				    it_should_behave_like "dataset modifying"
				    it_should_behave_like "clause-specific callspec"
				  end
			  end
		  end
    end
	    
	  describe "adding hints" do
	    def hints_to_check(ds, type, input)
	      ds.opts[:hints][type][(@orig_hints[type].length rescue 0), input.length]
	    end
	    COMMON_GROUP_BODY.call self, :hint
	  end
	  
	  describe "overwriting hints" do
	    def hints_to_check(ds, type, input)
	      ds.opts[:hints][type]
	    end
	    COMMON_GROUP_BODY.call self, :hints
	  end
	  
    describe "#hint_sql" do
	    it "generates clause-specific hint SQL" do
	      # set them all up front so we can test whether they get mixed up.
        TYPES.each do |type| @ds.hints! type, "hint for #{type}" end
	      TYPES.each do |type|
	        sql = (clause = type.to_s.upcase).dup
	        @ds.hint_sql(type, sql).should equal(sql)
	        sql.should == "#{clause} /*+ hint for #{type} */"
	      end
	    end
	    
	    it "skips empty hints" do
	      TYPES.each do |type|
	        sql = (clause = type.to_s.upcase).dup
	        @ds.hint_sql(type, sql).should be_nil
	        sql.should == clause
	      end
	    end
	    
      TYPES.each do |type|
		    it "is called by ##{type}_hint_sql" do
	        clause = type.to_s.upcase
		      @ds.should_receive(:hint_sql).with(type, clause)
		      @ds.__send__ :"#{type}_hint_sql", clause
		    end
		    it "is called by ##{type}_sql" do
	        args, clause = [], type.to_s.upcase
		      @ds.should_receive(:hint_sql).with(type, clause)
		      args.push :dual, :dual, :x if type == :merge
		      @ds.__send__ :"#{type}_sql", *args
		    end
	    end
    end
  end
end
