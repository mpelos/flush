module Flush
  class Client
    attr_reader :configuration, :sidekiq

    def initialize(configuration = Flush.configuration)
      @configuration = configuration
    end

    def configure
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
      if max_workflow_concurrency?(workflow)
        enqueue_workflow(workflow)
      else
        workflow.mark_as_started
        persist_workflow(workflow)
        push_workflow_in_running_queue(workflow)

        jobs = if job_names.empty?
                 workflow.initial_jobs
               else
                 job_names.map {|name| workflow.find_job(name) }
               end

        jobs.each do |job|
          enqueue_job(workflow.id, job)
        end
      end
    end

    def retry_workflow(workflow)
      if max_workflow_concurrency?(workflow)
        enqueue_workflow(workflow)
      else
        workflow.mark_as_started
        persist_workflow(workflow)
        push_workflow_in_running_queue(workflow)
        failed_jobs = workflow.jobs.select(&:failed?)

        failed_jobs.each do |job|
          enqueue_job(workflow.id, job, true)
        end
      end
    end

    def enqueue_workflow(workflow)
      push_workflow_in_waiting_queue(workflow)
      workflow.mark_as_enqueued
      persist_workflow(workflow)
    end

    def fail_workflow(workflow)
      start_enqueued_workflow(workflow) if workflow.queue
      workflow.mark_as_failed
    end

    def finish_workflow(workflow)
      workflow.mark_as_finished
      start_enqueued_workflow(workflow) if workflow.queue
    end

    def start_enqueued_workflow(workflow)
      return if workflow.queue.nil?

      remove_workflow_from_running_queue(workflow)
      waiting_queue = find_workflow_waiting_queue(workflow.queue)

      return nil if waiting_queue.empty?

      next_workflow = find_workflow(waiting_queue.first)
      remove_workflow_from_waiting_queue(workflow)

      if next_workflow.failed?
        retry_workflow(next_workflow)
      else
        start_workflow(next_workflow)
      end

      next_workflow
    end

    def stop_workflow(id)
      workflow = find_workflow(id)
      workflow.mark_as_stopped
      persist_workflow(workflow)
      start_enqueued_workflow(workflow) if workflow.queue
    end

    def max_workflow_concurrency?(workflow)
      return false if workflow.queue.nil?

      queue = find_workflow_running_queue(workflow.queue)
      queue.size >= configuration.workflow_concurrency_per_queue
    end

    def next_free_job_id(workflow_id,job_klass)
      job_identifier = nil
      loop do
        id = SecureRandom.uuid
        job_identifier = "#{job_klass}-#{id}"
        available = Sidekiq.redis do |redis|
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
        available = Sidekiq.redis do |redis|
          !redis.exists("flush.workflow.#{id}")
        end

        break if available
      end

      id
    end

    def all_workflows
      Sidekiq.redis do |redis|
        redis.keys("flush.workflows.*").map do |key|
          id = key.sub("flush.workflows.", "")
          find_workflow(id)
        end
      end
    end

    def find_workflow(id, parent = nil)
      Sidekiq.redis do |redis|
        data = redis.get("flush.workflows.#{id}")

        unless data.nil?
          hash = Flush::JSON.decode(data, symbolize_keys: true)
          keys = redis.keys("flush.jobs.#{id}.*")
          nodes = redis.mget(*keys).map { |json| Flush::JSON.decode(json, symbolize_keys: true) }
          workflow_from_hash(hash, nodes, parent)
        else
          raise WorkflowNotFound.new("Workflow with given id doesn't exist")
        end
      end
    end

    def persist_workflow(workflow)
      Sidekiq.redis do |redis|
        redis.set("flush.workflows.#{workflow.id}", workflow.to_json)
      end

      workflow.jobs.each {|job| persist_job(workflow.id, job) }
      workflow.mark_as_persisted
      true
    end

    def persist_job(workflow_id, job)
      Sidekiq.redis do |redis|
        redis.set("flush.jobs.#{workflow_id}.#{job.name}", job.to_json)
      end
    end

    def load_job(workflow_id, job_id)
      workflow = find_workflow(workflow_id)
      job_name_match = /(?<klass>\w*[^-])-(?<identifier>.*)/.match(job_id)
      hypen = '-' if job_name_match.nil?

      keys = Sidekiq.redis do |redis|
        redis.keys("flush.jobs.#{workflow_id}.#{job_id}#{hypen}*")
      end

      return nil if keys.nil?

      data = Sidekiq.redis do |redis|
        redis.get(keys.first)
      end

      return nil if data.nil?

      data = Flush::JSON.decode(data, symbolize_keys: true)
      Flush::Job.from_hash(workflow, data)
    end

    def destroy_workflow(workflow)
      Sidekiq.redis do |redis|
        redis.del("flush.workflows.#{workflow.id}")
      end
      workflow.jobs.each {|job| destroy_job(workflow.id, job) }
    end

    def destroy_job(workflow_id, job)
      Sidekiq.redis do |redis|
        redis.del("flush.jobs.#{workflow_id}.#{job.name}")
      end
    end

    def find_workflow_running_queue(queue_name)
      Sidekiq.redis do |redis|
        redis.smembers("flush.workflow_queues.running.#{queue_name}")
      end
    end

    def find_workflow_waiting_queue(queue_name)
      Sidekiq.redis do |redis|
        redis.smembers("flush.workflow_queues.waiting.#{queue_name}")
      end
    end

    def push_workflow_in_running_queue(workflow)
      Sidekiq.redis do |redis|
        redis.sadd("flush.workflow_queues.running.#{workflow.queue}", workflow.id)
      end
    end

    def push_workflow_in_waiting_queue(workflow)
      Sidekiq.redis do |redis|
        redis.sadd("flush.workflow_queues.waiting.#{workflow.queue}", workflow.id)
      end
    end

    def remove_workflow_from_running_queue(workflow)
      Sidekiq.redis do |redis|
        redis.srem("flush.workflow_queues.running.#{workflow.queue}", workflow.id)
      end
    end

    def remove_workflow_from_waiting_queue(workflow)
      Sidekiq.redis do |redis|
        redis.srem("flush.workflow_queues.waiting.#{workflow.queue}", workflow.id)
      end
    end

    def worker_report(message)
      report("flush.workers.status", message)
    end

    def workflow_report(message)
      report("flush.workflows.status", message)
    end

    def enqueue_job(workflow_id, job, retrying = false)
      job.enqueue!
      persist_job(workflow_id, job)
      Flush::Worker.perform_async workflow_id, job.name, retrying
    end

    private

    def workflow_from_hash(hash, nodes = nil, parent = nil)
      workflow = begin
        hash[:klass].constantize.new *hash[:arguments]
      rescue NameError => e
        anonymous_workflow.new *hash[:arguments]
      end

      workflow.jobs = []
      workflow.parent = parent
      workflow.stopped = hash.fetch(:stopped, false)
      workflow.id = hash[:id]
      workflow.enqueued_at = hash[:enqueued_at]
      workflow.scope = hash.fetch(:scope, {})
      workflow.scope[:promises] = build_promises(workflow.scope.fetch(:promises, {}))
      workflow.resolve_scope_promises!

      workflow.children = hash.fetch(:children_ids).map do |child_id|
        child = find_workflow(child_id, workflow)
        child
      end

      (nodes || hash[:nodes]).each do |node|
        workflow.jobs << Flush::Job.from_hash(workflow, node)
      end

      workflow
    end

    def anonymous_workflow
      Class.new(Workflow) do
        def configure(**kwargs)
        end
      end
    end

    def build_promises(promises)
      promises.each_with_object({}) do |(attr_name, params), acc|
        acc[attr_name] = PromiseAttribute.new(attr_name)
      end
    end

    def report(key, message)
      Sidekiq.redis do |redis|
        redis.publish(key, Flush::JSON.encode(message))
      end
    end
  end
end
