require 'spec_helper'

describe Flush::Configuration do

  it "has defaults set" do
    subject.flushfile = FLUSHFILE
    expect(subject.redis_url).to eq("redis://localhost:6379")
    expect(subject.concurrency).to eq(5)
    expect(subject.namespace).to eq('flush')
    expect(subject.flushfile).to eq(FLUSHFILE.realpath)
    expect(subject.environment).to eq('development')
  end

  describe "#configure" do
    it "allows setting options through a block" do
      Flush.configure do |config|
        config.redis_url = "redis://localhost"
        config.concurrency = 25
        config.environment = 'production'
      end

      expect(Flush.configuration.redis_url).to eq("redis://localhost")
      expect(Flush.configuration.concurrency).to eq(25)
      expect(Flush.configuration.environment).to eq('production')
    end
  end
end
