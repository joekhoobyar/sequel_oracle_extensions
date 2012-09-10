require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "sequel_oracle_extensions"
    gem.summary = %Q{Oracle MERGE, optimizer hints, and schema extensions for Sequel}
    gem.description = %Q{Oracle extensions for Sequel, including MERGE statements, optimizer hints, and schema extensions.}
    gem.email = "joe@ankhcraft.com"
    gem.homepage = "http://github.com/joekhoobyar/sequel_oracle_extensions"
    gem.authors = ["Joe Khoobyar"]
    gem.add_dependency "sequel", ">= 3.39" 
    gem.add_runtime_dependency "ruby-oci8", ">= 2.0"
    gem.add_development_dependency "rspec", ">= 2.0"
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:rspec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end
RSpec::Core::RakeTask.new(:rcov) do |spec|
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rcov = true
end

task :rspec => :check_dependencies

task :default => :rspec

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "sequel_oracle_extensions #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
