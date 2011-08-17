require 'rubygems'
require 'pp'
begin require 'win32console' and include Win32::Console::ANSI
rescue LoadError
end if RUBY_PLATFORM =~ /msvc|mingw|cygwin|win32/

$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'sequel'
require "rspec"

DATABASE_URL = begin
	user     = ENV['DATABASE_USER'] || 'hr'
	password = ENV['DATABASE_PASSWORD'] || 'hr'
	name     = ENV['DATABASE_NAME'] || 'xe'
	host     = ENV['DATABASE_HOST'] || 'localhost'
  port     = ':'+ENV['DATABASE_PORT'] rescue nil
  "oracle://#{user}:#{password}@#{host}#{port}/#{name}"
end

RSpec.configure do |config|
  require 'rspec/expectations'
  config.include RSpec::Matchers
  config.mock_with :rspec
end
