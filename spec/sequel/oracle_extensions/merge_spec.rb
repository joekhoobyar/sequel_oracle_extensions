require File.expand_path('../../../spec_helper', __FILE__)
require 'sequel/oracle_extensions/merge'

describe "Sequel::OracleExtensions::Merge" do
  before(:all) do
    @db = Sequel.connect(DATABASE_URL)
  end
end
