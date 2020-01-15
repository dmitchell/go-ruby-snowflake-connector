$LOAD_PATH << File.expand_path('../..', __FILE__)
require 'lib/go_snowflake_client'
require 'logger'

class CommonSampleInterface
  attr_reader :db_pointer

  def initialize(database)
    @logger = Logger.new(STDERR)

    @db_pointer = GoSnowflakeClient.connect(
      ENV['SNOWFLAKE_TEST_ACCOUNT'],
      ENV['SNOWFLAKE_TEST_WAREHOUSE'],
      database,
      ENV['SNOWFLAKE_TEST_SCHEMA'] || 'TPCDS_SF10TCL',
      ENV['SNOWFLAKE_TEST_USER'],
      ENV['SNOWFLAKE_TEST_PASSWORD'],
      ENV['SNOWFLAKE_TEST_ROLE'] || 'PUBLIC')

    log_error unless @db_pointer
  end

  def close_db
    GoSnowflakeClient.close(@db_pointer) if @db_pointer
  end

  def log_error
    @logger ||= Logger.new(STDERR)
    @logger.error(GoSnowflakeClient.last_error)
  end
end
