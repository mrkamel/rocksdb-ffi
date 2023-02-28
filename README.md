# rocksdb-ffi

RocksDB for ruby using FFI

## Installation

Install rocksdb and then install the gem and add to the application's Gemfile
by executing:

```
$ bundle add rocksdb-ffi
```

If bundler is not being used to manage dependencies, install the gem by executing:

```
$ gem install rocksdb-ffi
```

## Usage

rocksdb-ffi exposes the basic functionality of rocksdb.

```ruby
rocksdb = RocksDB.new("/path/to/db")

rocksdb.put("key", "value")

rocksdb.delete("key")

rocksdb.each do |key, value|
  # ...
end

rocksdb.each_key do |key|
  # ...
end

rocksdb.close

rocksdb.open("/path/to/other/db")
```

The library is tested against ruby 3.0/3.1/3.2 and rocksdb 7.2. That's it for now!

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run
`rake spec` to run the tests. You can also run `bin/console` for an interactive
prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To
release a new version, update the version number in `version.rb`, and then run
`bundle exec rake release`, which will create a git tag for the version, push
git commits and the created tag, and push the `.gem` file to
[rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/mrkamel/rocksdb-ffi. This project is intended to be a
safe, welcoming space for collaboration, and contributors are expected to
adhere to the [code of
conduct](https://github.com/mrkamel/rocksdb-ffi/blob/main/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the
[MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the rocksdb-ffi project's codebases, issue trackers,
chat rooms and mailing lists is expected to follow the
[code of conduct](https://github.com/mrkamel/rocksdb-ffi/blob/main/CODE_OF_CONDUCT.md).