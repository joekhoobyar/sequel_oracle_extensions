require File.expand_path('../../../spec_helper', __FILE__)
require 'sequel/oracle_extensions/schemata'

describe "Sequel::OracleExtensions::Schemata" do
  before(:all) do
    @db = Sequel.connect(DATABASE_URL)
  end
  
  it "is mixed into the base Oracle driver" do
    Sequel::Oracle::Database.included_modules.should be_include(Sequel::Oracle::DatabaseExtensions)
    Sequel::Schema::AlterTableGenerator.included_modules.should be_include(Sequel::Oracle::AlterTableExtensions)
  end

  share_examples_for "table schema" do
	  before(:all) do
	    @features = @columns.instance_variable_get :@features
	  end
	  
	  it "populates the @features hash" do
	    @features.should be_kind_of(Hash)
	    %w(clustered temporary partitioning typed index_only).each do |k|
		    @features.should be_include(k.to_sym)
		    [TrueClass, FalseClass, NilClass].should be_include(@features[k].class)
	    end
	    @features[:owner].tap do |k|
	      k.should be_kind_of(Symbol)
	      k.to_s.downcase.should == k.to_s
	    end
	  end
  end

	share_examples_for "column metadata" do
		it 'describes each column' do
	    seen = {}
	    @columns.each do |column|
	      column.length.should == 2
	      name, details = *column
	      
	      name.should be_kind_of(Symbol)
	      seen.should_not be_include(name)
	      seen[name] = true
	      
	      details.should be_kind_of(Hash)
	      %w( type db_type type_string charset_form char_used allow_null
	          char_size data_size precision scale fsprecision lfprecision ).each do |k|
	        details.should be_include(k.to_sym)
	      end
		  end
		end
	end
  
  describe "#schema_parse_table" do
    before(:all) do
	    @columns = @db.schema_parse_table "ALL_OBJECTS"
    end
    
    it 'returns a columns array' do
	    @columns.should be_kind_of(Array)
    end
    
    it_should_behave_like "table schema"
    it_should_behave_like "column metadata"
  end
  
  describe "#schema" do
    before(:all) do
	    @columns = @db.schema "ALL_OBJECTS", :reload=>true
    end
    
    it 'returns a columns array' do
	    @columns.should be_kind_of(Array)
    end
    
    it_should_behave_like "table schema"
    it_should_behave_like "column metadata"
  end
  
  describe "#table_metadata" do
    before(:all) do
      @metadata = @db.table_metadata :all_objects, :reload=>true
      @columns = @metadata[:columns]
    end
    
    it 'returns a metadata hash' do
      @metadata.should be_kind_of(Hash)
	    @columns.should be_kind_of(Array)
    end
    
    it_should_behave_like "column metadata"
  end

end
