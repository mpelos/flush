require "bundler/setup"

require "graphviz"
require "hiredis"
require "pathname"
require "redis"
require "securerandom"
require "sidekiq"
require "multi_json"

require "flush/json"
require "flush/cli"
require "flush/cli/overview"
require "flush/graph"
require "flush/client"
require "flush/configuration"
require "flush/errors"
require "flush/promise_attribute"
require "flush/job"
require "flush/worker"
require "flush/workflow"

module Flush
  def self.root
    Pathname.new(__FILE__).parent.parent
  end

  def self.configuration
    @configuration ||= Configuration.new
  end

  def self.configure
    yield configuration
  end
end
