# Assumes you have access to snowflake_sample_data https://docs.snowflake.net/manuals/user-guide/sample-data.html
# Set env vars: SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PASSWORD, SNOWFLAKE_TEST_WAREHOUSE
# optionally set SNOWFLAKE_TEST_SCHEMA, SNOWFLAKE_TEST_ROLE
$LOAD_PATH << File.expand_path('../..', __FILE__)
require 'lib/go_snowflake_client'
require 'logger'

class SnowflakeSampleData

  def initialize
    @logger = Logger.new(STDERR)

    @db_pointer = GoSnowflakeClient.connect(
      ENV['SNOWFLAKE_TEST_ACCOUNT'],
      ENV['SNOWFLAKE_TEST_WAREHOUSE'],
      "SNOWFLAKE_SAMPLE_DATA",
      ENV['SNOWFLAKE_TEST_SCHEMA'] || 'TPCDS_SF10TCL',
      ENV['SNOWFLAKE_TEST_USER'],
      ENV['SNOWFLAKE_TEST_PASSWORD'],
      ENV['SNOWFLAKE_TEST_ROLE'] || 'PUBLIC')

    log_error unless @db_pointer
  end

  def close_db
    GoSnowflakeClient.close(@db_pointer) if @db_pointer
  end

  def get_customer_names(where = "c_last_name = 'Flowers'")
    raise('db not connected') unless @db_pointer

    query = "select c_first_name, c_last_name from \"CUSTOMER\""
    query += " where #{where}" if where

    GoSnowflakeClient.select(@db_pointer, query) { |row| @logger.info("#{row[0]} #{row[1]}") }
  end

  # @example process_unshipped_web_sales {|row| check_shipping_queue(row)}
  def process_unshipped_web_sales(limit = 1_000, &block)
    raise('db not connected') unless @db_pointer
    query = <<~QUERY.freeze
      select c_first_name, c_last_name, ws_sold_date_sk, ws_list_price
      from "CUSTOMER" 
      inner join "WEB_SALES"
      ON c_customer_sk = ws_bill_customer_sk
      where ws_ship_date_sk is null
      #{"limit #{limit}" if limit}
    QUERY

    GoSnowflakeClient.select(@db_pointer, query, &block)
  end

  def log_error
    @logger ||= Logger.new(STDERR)
    @logger.error(GoSnowflakeClient.last_error)
  end
end
