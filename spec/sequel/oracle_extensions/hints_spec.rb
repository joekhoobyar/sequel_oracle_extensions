require File.expand_path('../../../spec_helper', __FILE__)
require 'sequel/oracle_extensions/merge'
require 'sequel/oracle_extensions/hints'

describe "Sequel::OracleExtensions::Hints" do
  before(:all) do
    @db = Sequel.connect(DATABASE_URL)
    @clauses = %w(SELECT INSERT UPDATE DELETE MERGE)
    @types = @clauses.map{|clause| clause.downcase.intern}
  end

  it "hooks into dataset clause methods" do
    [Sequel::Dataset, Sequel::Oracle::DatasetMethods].each do |klass|
	    @clauses.each do |clause|
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
    def apply_hints!(*args)
      @old_hints = @ds.opts[:hints]
      @new_ds    = @ds.__send__(@method, *args)

      @new_ds.should be_kind_of(Sequel::Dataset)
      @new_ds.opts[:hints].should_not be_empty
    end

	  it "1 argument form (String) applies :select hints" do
	    apply_hints! @hints.first
      hints_to_check(@new_ds, :select, @hints[0,1]).should == @hints[0,1]
	  end
	  it "1+ argument form (String, ...) applies :select hints" do
	    apply_hints! *@hints
      hints_to_check(@new_ds, :select, @hints).should == @hints
	  end
	  it "2 argument form (:clause, String)" do
	    @types.each do |type|
        apply_hints! type, @hints.first
	      hints_to_check(@new_ds, type, @hints[0,1]).should == @hints[0,1]
	    end
	  end
	  it "2+ argument form (:clause, String, ...)" do
	    @types.each do |type|
        apply_hints! type, *@hints
	      hints_to_check(@new_ds, type, @hints).should == @hints
	    end
	  end
  end
  
  describe "appends clause-specific hints" do
    before(:each){ @ds, @hints = @db[:dual], ['foo', 'bar'] }
    
    def hints_to_check(ds, type, input)
      ds.opts[:hints][type][(@orig_hints[type].length rescue 0), input.length]
    end

	  describe "#hint" do
	    it_should_behave_like "dataset cloning"
	    it_should_behave_like "standard callspec"
      before(:each){ @method = :hint }
	  end
	
	  describe "#hint!" do
	    it_should_behave_like "dataset modifying"
	    it_should_behave_like "standard callspec"
      before(:each){ @method = :hint! }
	  end
  end
  
  describe "sets clause-specific hints" do
    before(:each){ @ds, @hints = @db[:dual], ['foo', 'bar'] }
    
    def hints_to_check(ds, type, input)
      ds.opts[:hints][type]
    end

    describe "#hints" do
      it_should_behave_like "dataset cloning"
      it_should_behave_like "standard callspec"
      before(:each){ @method = :hints }
    end

    describe "#hints!" do
      it_should_behave_like "dataset modifying"
      it_should_behave_like "standard callspec"
      before(:each){ @method = :hints! }
    end
  end

end
