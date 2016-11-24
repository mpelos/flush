module Flush
  class Configuration
    attr_accessor :concurrency, :namespace, :redis_url, :environment

    def self.from_json(json)
      new(Flush::JSON.decode(json, symbolize_keys: true))
    end

    def initialize(hash = {})
      self.concurrency = hash.fetch(:concurrency, 5)
      self.namespace   = hash.fetch(:namespace, 'flush')
      self.redis_url   = hash.fetch(:redis_url, 'redis://localhost:6379')
      self.flushfile   = hash.fetch(:flushfile, 'Flushfile.rb')
      self.environment = hash.fetch(:environment, 'development')
    end

    def flushfile=(path)
      @flushfile = Pathname(path)
    end

    def flushfile
      @flushfile.realpath
    end

    def to_hash
      {
        concurrency: concurrency,
        namespace:   namespace,
        redis_url:   redis_url,
        environment: environment
      }
    end

    def to_json
      Flush::JSON.encode(to_hash)
    end
  end
end
