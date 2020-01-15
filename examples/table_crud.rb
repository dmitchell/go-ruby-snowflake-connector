require_relative 'common_sample_interface.rb' # Creates/uses test_data table in the db you point to
# Set env vars: SNOWFLAKE_TEST_ACCOUNT, SNOWFLAKE_TEST_USER, SNOWFLAKE_TEST_PASSWORD, SNOWFLAKE_TEST_WAREHOUSE, SNOWFLAKE_TEST_DATABASE
# optionally set SNOWFLAKE_TEST_SCHEMA, SNOWFLAKE_TEST_ROLE
# use GoSnowflakeClient.select(c.db_pointer, 'select * from test_table', field_count: 3).to_a to see the db contents
class TableCRUD < CommonSampleInterface

  TEST_TABLE_NAME = 'TEST_TABLE'

  def initialize
    super(ENV['SNOWFLAKE_TEST_DATABASE'])
  end

  def create_test_table
    command = <<~COMMAND
      CREATE TEMP TABLE IF NOT EXISTS #{TEST_TABLE_NAME}
      (id int AUTOINCREMENT NOT NULL,
       some_timestamp TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
       a_string string(20))
    COMMAND
    result = GoSnowflakeClient.exec(@db_pointer, command)
    result || log_error
  end

  # @example insert_test_table([['2019-07-04 04:12:31 +0000', 'foo'],['2019-07-04 04:12:31 -0600', 'bar'],[Time.now, 'quux']])
  def insert_test_table(time_string_pairs)
    command = <<~COMMAND
      INSERT INTO #{TEST_TABLE_NAME} (some_timestamp, a_string)
      VALUES #{time_string_pairs.map {|time, text| "('#{time}', '#{text}')"}.join(', ')}
    COMMAND
    result = GoSnowflakeClient.exec(@db_pointer, command)
    result || log_error
  end

  # @example update_test_table([[1, 'foo'],[99, 'bar'],[31, 'quux']])
  def update_test_table(id_string_pairs)
    id_string_pairs.map do |id, text|
      command = <<~COMMAND
        UPDATE #{TEST_TABLE_NAME} 
        SET a_string = '#{text}'
        WHERE id = #{id}
      COMMAND
      result = GoSnowflakeClient.exec(@db_pointer, command)
      result || log_error
    end
  end
end
