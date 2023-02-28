# frozen_string_literal: true

require "fileutils"

RSpec.describe RocksDB do
  after { FileUtils.rm_rf("/tmp/rocksdb") }

  it "has a version number" do
    expect(described_class::VERSION).not_to be nil
  end

  describe "#initialize" do
    it "opens the database" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key", "value")

      expect(db.get("key")).to eq("value")
    ensure
      db.close
    end

    it "raises when the database is already opened" do
      db = described_class.new("/tmp/rocksdb")

      expect { described_class.new("/tmp/rocksdb") }.to raise_error(described_class::Error, /No locks available/)
    ensure
      db.close
    end
  end

  describe "#close" do
    it "closes the database" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.put("key", "value") }.to raise_error(described_class::ClosedError)
    ensure
      db.close
    end
  end

  describe "#open" do
    it "opens the database" do
      db = described_class.new("/tmp/rocksdb1")
      db.open("/tmp/rocksdb2")
      db.put("key", "value")

      expect(db.get("key")).to eq("value")

      db.close

      db = described_class.new("/tmp/rocksdb1")

      expect(db.get("key")).to be_nil

      db = described_class.new("/tmp/rocksdb2")

      expect(db.get("key")).to eq("value")
    ensure
      db.close
    end
  end

  describe "#put" do
    it "writes the specified key/value pair" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      expect(db.get("key1")).to eq("value1")
      expect(db.get("key2")).to eq("value2")
      expect(db.get("key3")).to eq("value3")
    ensure
      db.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.put("key", "value") }.to raise_error(described_class::ClosedError)
    ensure
      db.close
    end
  end

  describe "#get" do
    it "reads the specified key and returns the value" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      expect(db.get("key1")).to eq("value1")
      expect(db.get("key2")).to eq("value2")
      expect(db.get("key3")).to eq("value3")
    ensure
      db.close
    end

    it "returns nil when the key does not exist" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key", "value")

      expect(db.get("unknown")).to be_nil
    ensure
      db.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.get("key") }.to raise_error(described_class::ClosedError)
    ensure
      db.close
    end
  end

  describe "#each" do
    it "iterates the key/value pairs" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      pairs = []

      db.each do |key, value|
        pairs << [key, value]
      end

      expect(pairs).to eq([["key1", "value1"], ["key2", "value2"], ["key3", "value3"]])
    ensure
      db.close
    end

    it "returns an enumerator when no block is given" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      expect(db.each).to be_instance_of(Enumerator)
      expect(db.each.to_a).to eq([["key1", "value1"], ["key2", "value2"], ["key3", "value3"]])
    ensure
      db.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.each }.to raise_error(described_class::ClosedError)
    ensure
      db.close
    end
  end

  describe "#each_key" do
    it "iterates the keys" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      keys = []

      db.each_key do |key|
        keys << key
      end

      expect(keys).to eq(["key1", "key2", "key3"])
    ensure
      db.close
    end

    it "returns an enumerator when no block is given" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      expect(db.each_key).to be_instance_of(Enumerator)
      expect(db.each_key.to_a).to eq(["key1", "key2", "key3"])
    ensure
      db.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.each_key }.to raise_error(described_class::ClosedError)
    ensure
      db.close
    end
  end
end