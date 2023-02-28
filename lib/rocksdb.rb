# frozen_string_literal: true

require "rocksdb/version"
require "ffi"

class RocksDB
  module Lib
    extend FFI::Library

    ffi_lib "rocksdb"

    attach_function :rocksdb_options_create, [], :pointer
    attach_function :rocksdb_options_destroy, [:pointer], :void
    attach_function :rocksdb_options_set_create_if_missing, [:pointer, :uchar], :void
    attach_function :rocksdb_open, [:pointer, :string, :strptr], :pointer
    attach_function :rocksdb_close, [:pointer], :void
    attach_function :rocksdb_writeoptions_create, [], :pointer
    attach_function :rocksdb_writeoptions_destroy, [:pointer], :void
    attach_function :rocksdb_put, [:pointer, :pointer, :string, :size_t, :string, :size_t, :pointer], :void
    attach_function :rocksdb_readoptions_create, [], :pointer
    attach_function :rocksdb_readoptions_destroy, [:pointer], :void
    attach_function :rocksdb_get, [:pointer, :pointer, :string, :size_t, :pointer, :pointer], :pointer
    attach_function :rocksdb_delete, [:pointer, :pointer, :string, :size_t, :pointer], :void
    attach_function :rocksdb_create_iterator, [:pointer, :pointer], :pointer
    attach_function :rocksdb_iter_valid, [:pointer], :uchar
    attach_function :rocksdb_iter_seek_to_first, [:pointer], :void
    attach_function :rocksdb_iter_next, [:pointer], :void
    attach_function :rocksdb_iter_key, [:pointer, :pointer], :pointer
    attach_function :rocksdb_iter_value, [:pointer, :pointer], :pointer
    attach_function :rocksdb_iter_destroy, [:pointer], :void
    attach_function :rocksdb_free, [:pointer], :void
  end

  class Error < StandardError; end
  class ClosedError < Error; end

  ENCODING = "UTF-8"

  def initialize(path)
    @create_options = FFI::AutoPointer.new(Lib.rocksdb_options_create, Lib.method(:rocksdb_options_destroy))
    Lib.rocksdb_options_set_create_if_missing(@create_options, 1)

    @read_options = FFI::AutoPointer.new(Lib.rocksdb_readoptions_create, Lib.method(:rocksdb_readoptions_destroy))
    @write_options = FFI::AutoPointer.new(Lib.rocksdb_writeoptions_create, Lib.method(:rocksdb_writeoptions_destroy))

    @closed = true

    # Pre-define and re-use those pointers for better performance
    @error = FFI::MemoryPointer.new(:pointer, 1)
    @key_length = FFI::MemoryPointer.new(:size_t, 1)
    @value_length = FFI::MemoryPointer.new(:size_t, 1)

    self.open(path)
  end

  def self.open(path)
    new(path)
  end

  def open(path)
    close

    @error = FFI::MemoryPointer.new(:pointer, 1)
    @db = FFI::AutoPointer.new(Lib.rocksdb_open(@create_options, path, @error), method(:auto_close))
    check_error(@error)
    @closed = false
  end

  def put(key, value)
    raise(ClosedError, "Database is closed") if @closed

    Lib.rocksdb_put(@db, @write_options, key, key.bytesize, value, value.bytesize, @error)
    check_error(@error)
  end

  def get(key)
    raise(ClosedError, "Database is closed") if @closed

    res = Lib.rocksdb_get(@db, @read_options, key, key.bytesize, @key_length, @error)
    check_error(@error)

    return if res.null?

    read_string(res, @key_length)
  end

  def delete(key)
    raise(ClosedError, "Database is closed") if @closed

    Lib.rocksdb_delete(@db, @write_options, key, key.bytesize, @error)
    check_error(@error)
  end

  def close
    return if @closed

    @closed = true
    Lib.rocksdb_close(@db)
  end

  def each
    raise(ClosedError, "Database is closed") if @closed

    return enum_for(__method__) unless block_given?

    iterate do |iterator|
      key = read_string(Lib.rocksdb_iter_key(iterator, @key_length), @key_length)
      value = read_string(Lib.rocksdb_iter_value(iterator, @value_length), @value_length)

      yield(key, value)
    end
  end

  alias_method :each_pair, :each

  def each_key
    raise(ClosedError, "Database is closed") if @closed

    return enum_for(__method__) unless block_given?

    iterate do |iterator|
      key = read_string(Lib.rocksdb_iter_key(iterator, @key_length), @key_length)

      yield(key)
    end
  end

  private

  def iterate
    iterator = Lib.rocksdb_create_iterator(@db, @read_options)

    begin
      Lib.rocksdb_iter_seek_to_first(iterator)

      while Lib.rocksdb_iter_valid(iterator) != 0
        yield(iterator)

        Lib.rocksdb_iter_next(iterator)
      end
    ensure
      Lib.rocksdb_iter_destroy(iterator)
    end
  end

  def auto_close(*args)
    close
  end

  def check_error(error)
    pointer = error.read_pointer
    return if pointer.null?

    message = pointer.read_string.force_encoding(ENCODING)
    Lib.rocksdb_free(pointer)
    raise(Error, message)
  end

  def read_string(pointer, length_pointer)
    pointer.read_string(length_pointer.read(:size_t)).force_encoding(ENCODING)
  end
end
