require 'flush'
require 'fakeredis'
require 'sidekiq/testing'

Sidekiq::Testing.fake!
Sidekiq::Logging.logger = nil

class Prepare < Flush::Job; end
class FetchFirstJob < Flush::Job; end
class FetchSecondJob < Flush::Job; end
class PersistFirstJob < Flush::Job; end
class PersistSecondJob < Flush::Job; end
class NormalizeJob < Flush::Job; end
class BobJob < Flush::Job; end

FLUSHFILE  = Pathname.new(__FILE__).parent.join("Flushfile.rb")

class TestWorkflow < Flush::Workflow
  def configure
    run Prepare

    run NormalizeJob

    run FetchFirstJob,   after: Prepare
    run FetchSecondJob,  after: Prepare, before: NormalizeJob

    run PersistFirstJob, after: FetchFirstJob, before: NormalizeJob

  end
end

class ParameterTestWorkflow < Flush::Workflow
  def configure(param)
    run Prepare if param
  end
end

class Redis
  def publish(*)
  end
end

REDIS_URL = "redis://localhost:6379/12"

module FlushHelpers
  def redis
    @redis ||= Redis.new(url: REDIS_URL)
  end

  def jobs_with_id(jobs_array)
    jobs_array.map {|job_name| job_with_id(job_name) }
  end

  def job_with_id(job_name)
    /#{job_name}-(?<identifier>.*)/
  end
end

RSpec::Matchers.define :have_jobs do |flow, jobs|
  match do |actual|
    expected = jobs.map do |job|
      hash_including("args" => include(flow, job))
    end
    expect(Flush::Worker.jobs).to match_array(expected)
  end

  failure_message do |actual|
    "expected queue to have #{jobs}, but instead has: #{actual.jobs.map{ |j| j["args"][1]}}"
  end
end

RSpec.configure do |config|
  config.include FlushHelpers

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.before(:each) do
    Flush.configure do |config|
      config.redis_url = REDIS_URL
      config.environment = 'test'
      config.flushfile = FLUSHFILE
    end
  end


  config.after(:each) do
    Sidekiq::Worker.clear_all
    redis.flushdb
  end
end
