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
    attach_function :rocksdb_flush, [:pointer, :pointer, :pointer], :void
    attach_function :rocksdb_flushoptions_create, [], :pointer
    attach_function :rocksdb_flushoptions_destroy, [:pointer], :void
    attach_function :rocksdb_free, [:pointer], :void

    def self.auto_close(closed_state)
      proc do |db|
        Lib.rocksdb_close(db) unless closed_state.value
      end
    end
  end

  class Error < StandardError; end
  class ClosedError < Error; end

  class ClosedState
    attr_accessor :value

    def initialize(value)
      @value = value
    end
  end

  ENCODING = "UTF-8"

  def initialize(path)
    # For performance reasons, we create and re-use these options

    @create_options = FFI::AutoPointer.new(Lib.rocksdb_options_create, Lib.method(:rocksdb_options_destroy))
    Lib.rocksdb_options_set_create_if_missing(@create_options, 1)

    @read_options = FFI::AutoPointer.new(Lib.rocksdb_readoptions_create, Lib.method(:rocksdb_readoptions_destroy))
    @write_options = FFI::AutoPointer.new(Lib.rocksdb_writeoptions_create, Lib.method(:rocksdb_writeoptions_destroy))
    @flush_options = FFI::AutoPointer.new(Lib.rocksdb_flushoptions_create, Lib.method(:rocksdb_flushoptions_destroy))

    # To automatically close the database during cleanup we need an object, but
    # for fast checking we use the literal

    @closed_state = ClosedState.new(true)
    @closed = true

    open(path) # rubocop:disable Security/Open
  end

  def self.open(path)
    new(path)
  end

  def open(path)
    close

    error = FFI::MemoryPointer.new(:pointer, 1)
    @db = FFI::AutoPointer.new(Lib.rocksdb_open(@create_options, path, error), Lib.auto_close(@closed_state))
    check_error(error)

    @closed_state.value = false
    @closed = false
  end

  def flush
    raise(ClosedError, "Database is closed") if @closed

    error = FFI::MemoryPointer.new(:pointer, 1)
    Lib.rocksdb_flush(@db, @flush_options, error)
    check_error(error)
  end

  def put(key, value)
    raise(ClosedError, "Database is closed") if @closed

    error = FFI::MemoryPointer.new(:pointer, 1)
    Lib.rocksdb_put(@db, @write_options, key, key.bytesize, value, value.bytesize, error)
    check_error(error)
  end

  def get(key)
    raise(ClosedError, "Database is closed") if @closed

    length = FFI::MemoryPointer.new(:size_t, 1)
    error = FFI::MemoryPointer.new(:pointer, 1)
    res = Lib.rocksdb_get(@db, @read_options, key, key.bytesize, length, error)
    check_error(error)

    return if res.null?

    read_string(res, length)
  end

  def delete(key)
    raise(ClosedError, "Database is closed") if @closed

    error = FFI::MemoryPointer.new(:pointer, 1)
    Lib.rocksdb_delete(@db, @write_options, key, key.bytesize, error)
    check_error(error)
  end

  def close
    return if @closed

    Lib.rocksdb_close(@db)

    @closed_state.value = true
    @closed = true
  end

  def each
    raise(ClosedError, "Database is closed") if @closed

    return enum_for(__method__) unless block_given?

    iterate do |iterator|
      key_length = FFI::MemoryPointer.new(:size_t, 1)
      key = read_string(Lib.rocksdb_iter_key(iterator, key_length), key_length)

      value_length = FFI::MemoryPointer.new(:size_t, 1)
      value = read_string(Lib.rocksdb_iter_value(iterator, value_length), value_length)

      yield(key, value)
    end
  end

  alias_method :each_pair, :each

  def each_key
    raise(ClosedError, "Database is closed") if @closed

    return enum_for(__method__) unless block_given?

    iterate do |iterator|
      key_length = FFI::MemoryPointer.new(:size_t, 1)
      key = read_string(Lib.rocksdb_iter_key(iterator, key_length), key_length)

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
