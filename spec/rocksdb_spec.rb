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
      db&.close
    end

    it "raises when the database is already opened" do
      db = described_class.new("/tmp/rocksdb")

      expect { described_class.new("/tmp/rocksdb") }.to raise_error(described_class::Error, /No locks available/)
    ensure
      db&.close
    end
  end

  describe ".auto_close" do
    it "closes the database" do
      # Can't be tested properly
    end

    it "does not close an already closed database" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      allow(described_class::Lib).to receive(:rocksdb_close).and_call_original

      db = nil # rubocop:disable Lint/UselessAssignment

      GC.start

      expect(described_class::Lib).not_to have_received(:rocksdb_close)
    end
  end

  describe ".open" do
    it "delegates to new" do
      allow(described_class).to receive(:new)

      described_class.open("/tmp/rocksdb")

      expect(described_class).to have_received(:new).with("/tmp/rocksdb")
    end
  end

  describe "#close" do
    it "closes the database" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.put("key", "value") }.to raise_error(described_class::ClosedError)
    ensure
      db&.close
    end
  end

  describe "#flush" do
    it "flushes the database" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key", "value")
      db.flush

      FileUtils.cp_r("/tmp/rocksdb", "/tmp/rocksdb2")

      db2 = described_class.new("/tmp/rocksdb2")

      expect(db2.get("key")).to eq("value")
    ensure
      db&.close
      db2&.close

      FileUtils.rm_rf("/tmp/rocksdb2")
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.flush }.to raise_error(described_class::ClosedError)
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
      db&.close

      FileUtils.rm_rf("/tmp/rocksdb1")
      FileUtils.rm_rf("/tmp/rocksdb2")
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
      db&.close
    end

    it "handles utf-8 correctly" do
      db = described_class.new("/tmp/rocksdb")
      db.put("ÄÖÜßäöü", "value")
      db.put("key", "ÄÖÜßäöü")

      expect(db.get("ÄÖÜßäöü")).to eq("value")
      expect(db.get("ÄÖÜßäöü").encoding).to eq(Encoding::UTF_8)

      expect(db.get("key")).to eq("ÄÖÜßäöü")
      expect(db.get("key").encoding).to eq(Encoding::UTF_8)
    ensure
      db&.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.put("key", "value") }.to raise_error(described_class::ClosedError)
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
      db&.close
    end

    it "returns nil when the key does not exist" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key", "value")

      expect(db.get("unknown")).to be_nil
    ensure
      db&.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.get("key") }.to raise_error(described_class::ClosedError)
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
      db&.close
    end

    it "returns an enumerator when no block is given" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      expect(db.each).to be_instance_of(Enumerator)
      expect(db.each.to_a).to eq([["key1", "value1"], ["key2", "value2"], ["key3", "value3"]])
    ensure
      db&.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.each }.to raise_error(described_class::ClosedError)
    end
  end
  describe "#each_pair" do
    it "delegates to each" do
      db = described_class.new("/tmp/rocksdb")

      allow(db).to receive(:each)

      db.each_pair.to_a

      expect(db).to have_received(:each)
    ensure
      db&.close
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
      db&.close
    end

    it "returns an enumerator when no block is given" do
      db = described_class.new("/tmp/rocksdb")
      db.put("key1", "value1")
      db.put("key2", "value2")
      db.put("key3", "value3")

      expect(db.each_key).to be_instance_of(Enumerator)
      expect(db.each_key.to_a).to eq(["key1", "key2", "key3"])
    ensure
      db&.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.each_key }.to raise_error(described_class::ClosedError)
    end
  end

  describe "#each_prefix" do
    it "iterates the key/value pairs matching the prefix" do
      db = described_class.new("/tmp/rocksdb")
      db.put("prefix1:key1", "value1")
      db.put("prefix1:key2", "value2")
      db.put("prefix2:key3", "value3")
      db.put("prefix1:key4", "value4")
      db.put("prefix2:key5", "value5")

      pairs = []

      db.each_prefix("prefix1") do |key, value|
        pairs << [key, value]
      end

      expect(pairs).to eq([["prefix1:key1", "value1"], ["prefix1:key2", "value2"], ["prefix1:key4", "value4"]])
    ensure
      db&.close
    end

    it "returns an enumerator when no block is given" do
      db = described_class.new("/tmp/rocksdb")
      db.put("prefix1:key1", "value1")
      db.put("prefix1:key2", "value2")
      db.put("prefix2:key3", "value3")
      db.put("prefix1:key4", "value4")
      db.put("prefix2:key5", "value5")

      expect(db.each_prefix("prefix1").to_a).to eq([["prefix1:key1", "value1"], ["prefix1:key2", "value2"], ["prefix1:key4", "value4"]])
      expect(db.each_prefix("prefix2").to_a).to eq([["prefix2:key3", "value3"], ["prefix2:key5", "value5"]])
    ensure
      db&.close
    end

    it "raises when the database is closed" do
      db = described_class.new("/tmp/rocksdb")
      db.close

      expect { db.each_prefix("prefix") }.to raise_error(described_class::ClosedError)
    end
  end
end
