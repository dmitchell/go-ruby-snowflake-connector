lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ruby_snowflake_client/version'

Gem::Specification.new do |s|
  s.name    = "ruby_snowflake_client"
  s.version = GoSnowflakeClient::VERSION
  s.summary = "Snowflake connect for Ruby"
  s.author  = "CarGurus"
  s.email   = ['dmitchell@cargurus.com', 'sabbott@cargurus.com']
  s.platform = Gem::Platform::CURRENT
  s.description = <<~DESC
    Uses gosnowflake to connect to and communicate with Snowflake.
    This library is much faster than using ODBC especially for large result sets and avoids ODBC butchering of timezones.
  DESC
  s.license = 'MIT'  # TODO double check

  s.files = ['ext/ruby_snowflake_client.h', 'ext/ruby_snowflake_client.so', 'lib/go_snowflake_client.rb', 'lib/ruby_snowflake_client/version.rb']

  # perhaps nothing and figure out how to build and pkg the platform specific .so, or .a, or ...
  #s.extensions << "ext/ruby_snowflake_client/extconf.rb"

  s.add_dependency 'ffi'
  s.add_development_dependency "bundler"
  s.add_development_dependency "rake"
end
