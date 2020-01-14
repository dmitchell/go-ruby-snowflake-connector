$LOAD_PATH << File.dirname(__FILE__)
require 'ruby_snowflake_client/version'
require 'ffi'

# Note: this library is not thread safe as it caches the db and last error
# The call pattern expectation is to call last_error after any call which may have gotten an error. If last_error is
# `nil`, there was no error.
module GoSnowflakeClient
  extend self

  # @return String last error or nil. May be end of file which is not really an error
  def last_error()
    error, cptr = GoSnowflakeClientBinding.last_error
    LibC.free(cptr) if error
    error
  end

  # @param account[String] should include everything in the db url ahead of region.snowflakecomputing.com
  # @param port[Integer]
  # @return query_object[Pointer] a pointer to use for subsequent calls not inspectable nor viewable by Ruby
  def connect(account, warehouse, database, schema, user, password, role, port = 443)
    GoSnowflakeClientBinding.connect(account, warehouse, database, schema, user, password, role, port || 443)
  end

  # @param db_pointer[Pointer] the pointer which `connect` returned.
  def close(db_pointer)
    GoSnowflakeClientBinding.close(db_pointer)
  end

  # @param db_pointer[Pointer] the pointer which `connect` returned.
  # @param statement[String] an executable query which should return number of rows affected
  # @return rowcount[Number] number of rows or nil if there was an error
  def exec(db_pointer, statement)
    count = GoSnowflakeClientBinding.exec(db_pointer, statement)  # returns -1 for error
    count >= 0 ? count : nil
  end

  # Send a query and then yield each row as an array of strings to the given block
  # @param db_pointer[Pointer] the pointer which `connect` returned.
  # @param query[String] a select query to run.
  # @return error_string
  # @yield List<String>
  def select(db_pointer, sql, field_count: nil)
    return nil unless db_pointer
    return to_enum(__method__, db_pointer, sql) unless block_given?

    query_pointer = fetch(db_pointer, sql)
    return nil if query_pointer.nil? || query_pointer == FFI::Pointer::NULL

    field_count ||= column_count(query_pointer)
    loop do
      row = get_next_row(query_pointer, field_count)
      return last_error unless row

      yield row
    end
  end

  # @param db_pointer[Pointer] the pointer which `connect` returned.
  # @param query[String] a select query to run.
  # @return query_object[Pointer] a pointer to use for subsequent calls not inspectable nor viewable by Ruby; however,
  #   if it's `nil`, check `last_error`
  def fetch(db_pointer, query)
    GoSnowflakeClientBinding.fetch(db_pointer, query)
  end

  # @param query_object[Pointer] the pointer which `fetch` returned. Go will gc this object when the query is done; so,
  #   don't expect to reference it after the call which returned `nil`
  # @param field_count[Integer] column count: it will seg fault if you provide a number greater than the actual number.
  #    Using code should use wrap this in something like
  #
  # @return [List<String>] the column values in order
  def get_next_row(query_object, field_count)
    raw_row = GoSnowflakeClientBinding.next_row(query_object)
    return nil if raw_row.nil? || raw_row == FFI::Pointer::NULL

    raw_row.get_array_of_pointer(0, field_count).map do |cstr|
      if cstr == FFI::Pointer::NULL || cstr.nil?
        nil
      else
        str = cstr.read_string
        LibC.free(cstr)
        str
      end
    end
  ensure
    LibC.free(raw_row) if raw_row
  end

  # @param query_object[Pointer] the pointer which `fetch` returned.
  # @return [List<String>] the column values in order
  def column_names(query_object, field_count = nil)
    raw_row = GoSnowflakeClientBinding.query_columns(query_object)
    return nil if raw_row.nil? || raw_row == FFI::Pointer::NULL

    raw_row.get_array_of_pointer(0, field_count).map do |cstr|
      if cstr == FFI::Pointer::NULL || cstr.nil?
        nil
      else
        str = cstr.read_string
        LibC.free(cstr)
        str
      end
    end
  ensure
    LibC.free(raw_row) if raw_row
  end

  # @param query_object[Pointer] the pointer which `fetch` returned.
  def column_count(query_object)
    GoSnowflakeClientBinding.query_column_count(query_object)
  end

  # TODO write query method which takes block and iterates with an ensure to tell go to release query_object and that
  # takes a list of converters for casting strings to intended types

  module LibC
    extend FFI::Library
    ffi_lib(FFI::Library::LIBC)

    attach_function(:free, [:pointer], :void)
  end

  module GoSnowflakeClientBinding
    extend FFI::Library

    POINTER_SIZE = FFI.type_size(:pointer)

    ffi_lib(File.expand_path('../ext/ruby_snowflake_client.so', File.dirname(__FILE__)))
    attach_function(:last_error, 'LastError', [], :strptr)
    # ugh, `port` in gosnowflake is just :int; however, ruby - ffi -> go is passing 32bit int if I just decl :int.
    attach_function(:connect, 'Connect', [:string, :string, :string, :string, :string, :string, :string, :int64], :pointer)
    attach_function(:close, 'Close', [:pointer], :void)
    attach_function(:exec, 'Exec', [:pointer, :string], :int64)
    attach_function(:fetch, 'Fetch', [:pointer, :string], :pointer)
    attach_function(:next_row, 'NextRow', [:pointer], :pointer)
    attach_function(:query_columns, 'QueryColumns', [:pointer], :pointer)
    attach_function(:query_column_count, 'QueryColumnCount', [:pointer], :int32)
  end
end
