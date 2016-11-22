module Flush
  class Client
    attr_reader :configuration, :sidekiq

    def initialize(config = Flush.configuration)
      @configuration = config
      @sidekiq = build_sidekiq
    end

    def configure
      yield configuration
      @sidekiq = build_sidekiq
    end

    def create_workflow(name)
      begin
        name.constantize.create
      rescue NameError
        raise WorkflowNotFound.new("Workflow with given name doesn't exist")
      end
      flow
    end

    def start_workflow(workflow, job_names = [])
      workflow.mark_as_started
      persist_workflow(workflow)

      jobs = if job_names.empty?
               workflow.initial_jobs
             else
               job_names.map {|name| workflow.find_job(name) }
             end

      jobs.each do |job|
        enqueue_job(workflow.id, job)
      end
    end

    def stop_workflow(id)
      workflow = find_workflow(id)
      workflow.mark_as_stopped
      persist_workflow(workflow)
    end

    def next_free_job_id(workflow_id,job_klass)
      job_identifier = nil
      loop do
        id = SecureRandom.uuid
        job_identifier = "#{job_klass}-#{id}"
        available = connection_pool.with do |redis|
          !redis.exists("flush.jobs.#{workflow_id}.#{job_identifier}")
        end

        break if available
      end

      job_identifier
    end

    def next_free_workflow_id
      id = nil
      loop do
        id = SecureRandom.uuid
        available = connection_pool.with do |redis|
          !redis.exists("flush.workflow.#{id}")
        end

        break if available
      end

      id
    end

    def all_workflows
      connection_pool.with do |redis|
        redis.keys("flush.workflows.*").map do |key|
          id = key.sub("flush.workflows.", "")
          find_workflow(id)
        end
      end
    end

    def find_workflow(id)
      connection_pool.with do |redis|
        data = redis.get("flush.workflows.#{id}")

        unless data.nil?
          hash = Flush::JSON.decode(data, symbolize_keys: true)
          keys = redis.keys("flush.jobs.#{id}.*")
          nodes = redis.mget(*keys).map { |json| Flush::JSON.decode(json, symbolize_keys: true) }
          workflow_from_hash(hash, nodes)
        else
          raise WorkflowNotFound.new("Workflow with given id doesn't exist")
        end
      end
    end

    def persist_workflow(workflow)
      connection_pool.with do |redis|
        redis.set("flush.workflows.#{workflow.id}", workflow.to_json)
      end

      workflow.jobs.each {|job| persist_job(workflow.id, job) }
      workflow.mark_as_persisted
      true
    end

    def persist_job(workflow_id, job)
      connection_pool.with do |redis|
        redis.set("flush.jobs.#{workflow_id}.#{job.name}", job.to_json)
      end
    end

    def load_job(workflow_id, job_id)
      workflow = find_workflow(workflow_id)
      job_name_match = /(?<klass>\w*[^-])-(?<identifier>.*)/.match(job_id)
      hypen = '-' if job_name_match.nil?

      keys = connection_pool.with do |redis|
        redis.keys("flush.jobs.#{workflow_id}.#{job_id}#{hypen}*")
      end

      return nil if keys.nil?

      data = connection_pool.with do |redis|
        redis.get(keys.first)
      end

      return nil if data.nil?

      data = Flush::JSON.decode(data, symbolize_keys: true)
      Flush::Job.from_hash(workflow, data)
    end

    def destroy_workflow(workflow)
      connection_pool.with do |redis|
        redis.del("flush.workflows.#{workflow.id}")
      end
      workflow.jobs.each {|job| destroy_job(workflow.id, job) }
    end

    def destroy_job(workflow_id, job)
      connection_pool.with do |redis|
        redis.del("flush.jobs.#{workflow_id}.#{job.name}")
      end
    end

    def worker_report(message)
      report("flush.workers.status", message)
    end

    def workflow_report(message)
      report("flush.workflows.status", message)
    end

    def enqueue_job(workflow_id, job)
      job.enqueue!
      persist_job(workflow_id, job)

      sidekiq.push(
        'class' => Flush::Worker,
        'queue' => configuration.namespace,
        'args'  => [workflow_id, job.name]
      )
    end

    private

    def workflow_from_hash(hash, nodes = nil)
      flow = hash[:klass].constantize.new *hash[:arguments]
      flow.jobs = []
      flow.stopped = hash.fetch(:stopped, false)
      flow.id = hash[:id]

      (nodes || hash[:nodes]).each do |node|
        flow.jobs << Flush::Job.from_hash(flow, node)
      end

      flow
    end

    def report(key, message)
      connection_pool.with do |redis|
        redis.publish(key, Flush::JSON.encode(message))
      end
    end


    def build_sidekiq
      Sidekiq::Client.new(connection_pool)
    end

    def build_redis
      Redis.new(url: configuration.redis_url)
    end

    def connection_pool
      @connection_pool ||= ConnectionPool.new(size: configuration.concurrency, timeout: 1) { build_redis }
    end
  end
end
