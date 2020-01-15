# Snowflake Connector for Ruby

Uses [gosnowflake](https://github.com/snowflakedb/gosnowflake/) to more efficiently query snowflake than ODBC. We found
at least 2 significant problems with ODBC which this resolves:
1. For large result sets, ODBC would get progressively slower per row as it would retrieve all the preceding 
pages in order to figure out the offset. This new gem uses a streaming interface alleviating the need for 
offsets and limit when paging through result sets.
2. ODBC mangled timezone information.

In addition, this gem is a lot faster for all but the most trivial queries.

## Tech Overview

This gem works by deserializing each row into an array of strings in Go. It then converts it to an array
of C strings (`**C.Char`) which it passes back through the FFI (foreign function interface) to Ruby.
There's a slight penalty for the 4 time type conversion (from the db type to Go string, from Go string
to C string, from C string to the Ruby string, and then from Ruby string to your intended type).

## How to use

Look at [examples](https://github.com/dmitchell/go-ruby-snowflake-connector/blob/master/examples)

1. add as gem to your project (`gem 'ruby_snowflake_client', '~> 0.2.2'`)
2. put `require 'go_snowflake_client'` at the top of your files which use it
3. following the pattern of the [example connect](https://github.com/dmitchell/go-ruby-snowflake-connector/blob/master/examples/table_crud.rb), 
call `GoSnowflakeClient.connect` with your database information and credentials.
4. use `GoSnowflakeClient.exec` to execute create, update, delete, and insert queries. If it
returns `nil`, call `GoSnowflakeClient.last_error` to get the error. Otherwise, it will return
the number of affected rows.
5. use `GoSnowflakeClient.select` with a block to execute on each row to query the database. This
will return either `nil` or an error string.
9. and finally, call `GoSnowflakeClient.close(db_pointer)` to close the database connection

### Our use pattern

In our application, we've wrapped this library with query generators and model definitions somewhat ala
Rails but with less dynamic introspection although we could add it by using 
``` ruby
GoSnowflakeClient.select(db, 'describe table my_table') do |col_name, col_type, _, nullable, *_| 
    my_table.add_column_description(col_name, col_type, nullable)
end
```

Each snowflake model class inherits from an abstract class which instantiates model instances
from each query by a pattern like
``` ruby
  GoSnowflakeClient.select(db, query) do |row|
    entry = self.new(fields.zip(row).map {|field, value| cast(field, value)}.to_h)
    yield entry
  end

  def cast(field_name, value)
    if value.nil?
      [field_name, value]
    elsif column_name_to_cast.include?(field_name)
      cast_method = column_name_to_cast[field_name]
      if cast_method == :to_time
        [field_name, value.to_time(:local)]
      elsif cast_method == :to_utc
        [field_name, value.to_time(:utc)]
      elsif cast_method == :to_date
        [field_name, value.to_date]
      elsif cast_method == :to_utc_date
        [field_name, value.to_time(:utc).to_date]
      else
        [field_name, value.public_send(cast_method)]
      end
    else
      [field_name, value]
    end
  end

# where each model declares column_name_to_cast ala
  COLUMN_NAME_TO_CAST = {
      id: :to_i,
      ad_text_id: :to_i,
      is_mobile: :to_bool,
      is_full_site: :to_bool,
      action_element_count: :to_i,
      created_at: :to_time,
      session_idx: :to_i,
      log_idx: :to_i,
      log_date: :to_utc_date}.with_indifferent_access.freeze

  def self.column_name_to_cast
    COLUMN_NAME_TO_CAST
  end
```

Of course, instantiating an object for each row adds expense and gc stress; so, it may not always
be a good approach.
