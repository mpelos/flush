module Flush
  class JSON
    def self.encode(data)
      MultiJson.dump(data)
    end

    def self.decode(data, options = {})
      MultiJson.load(data, options)
    end
  end
end
