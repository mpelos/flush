require "securerandom"

module Flush
  class Workflow
    attr_accessor :id, :jobs, :stopped, :persisted, :arguments, :parent, :scope, :enqueued_at
    attr_writer :children

    def initialize(*args)
      @id = id
      @jobs = []
      @dependencies = []
      @persisted = false
      @stopped = false
      @arguments = args

      if @arguments.any?
        @scope = @arguments.last.each_with_object({}) do |(attr_name, attribute), acc|
          if attribute.is_a? PromiseAttribute
            acc[:promises] ||= {}
            acc[:promises][attr_name] = attribute
          else
            acc[attr_name] = attribute
          end
        end
      end

      setup
      after_initialize(*@arguments)
    end

    def self.flush_options(options = {})
      @@queue = options[:queue]
    end

    def self.find(id)
      client.find_workflow(id)
    end

    def self.create(*args)
      flow = new(*args)
      flow.save
      flow
    end

    def after_initialize(*args)
    end

    def on_enqueue
    end

    def on_fail
    end

    def on_success
    end

    def queue
      @@queue ||= nil
      @@queue
    end

    def retry
      client.retry_workflow(self)
    end

    def save
      persist!
    end

    def configure(*args)
    end

    def start
      persist!
      client.start_workflow(self)
      id
    end

    def persist!
      children.each { |child| child.persist! }
      client.persist_workflow(self)
    end

    def mark_as_persisted
      @persisted = true
    end

    def mark_as_started
      @stopped = false
      self.enqueued_at = nil
    end

    def mark_as_stopped
      @stopped = true
    end

    def mark_as_enqueued
      self.enqueued_at = current_timestamp
      on_enqueue
    end

    def mark_as_finished
      on_success
    end

    def mark_as_failed
      on_fail
    end

    def resolve_dependencies
      @dependencies.each do |dependency|
        from = find_job(dependency[:from])
        to   = find_job(dependency[:to])

        to.incoming << dependency[:from]
        from.outgoing << dependency[:to]
      end
    end

    def find_job(name)
      if parent
        parent.find_job(name)
      else
        match_data = /(?<klass>\w*[^-])-(?<identifier>.*)/.match(name.to_s)
        if match_data.nil?
          job = jobs.find { |node| node.class.to_s == name.to_s }
        else
          job = jobs.find { |node| node.name.to_s == name.to_s }
        end
        job
      end
    end

    def pending?
      !started_at? && !failed?
    end

    def enqueued?
      !!enqueued_at
    end

    def started?
      !!started_at
    end

    def running?
      started? && !finished?
    end

    def failed?
      jobs.any?(&:failed?)
    end

    def stopped?
      stopped
    end

    def finished?
      jobs.all?(&:finished?)
    end

    def succeeded?
      jobs.all?(&:finished?) && jobs.none?(&:failed?)
    end

    def merge_scope(hash)
      scope.merge! hash
    end

    def find_in_descendants(workflow_id)
      if id == workflow_id
        self
      elsif children.size > 0
        children.find { |child| child.find_in_descendants(workflow_id) }
      end
    end

    def run(klass, options = {})
      # TODO: validates attribute accessor on requires
      node_promises = Array.wrap(options.fetch(:requires, [])).each_with_object({}) do |attr_name, acc|
        acc[attr_name.to_sym] = PromiseAttribute.new(attr_name)
      end
      node_params = options.fetch(:params, {}).each_with_object({}) do |(attr_name, attribute), acc|
        if attribute.is_a? PromiseAttribute
          node_promises[attr_name] = attribute
        else
          acc[attr_name] = attribute
        end
      end

      begin
        node = klass.new(**node_params.merge(node_promises))
      rescue ArgumentError => error
        if error.message.include? "missing keywords:"
          absent_args = error.message.split(": ").last.split(", ")
          raise MissingRequiredJobArguments,
            "The job requires parameters that was not injected by the workflow; " +
            "workflow: #{self.class.name || "AnonymousWorkflow"}, job: #{klass.name}, absent_args: #{absent_args}"
        else
          raise error
        end
      end

      node.setup(self, {
        name: client.next_free_job_id(id, klass.to_s),
        params: node_params,
        promises: node_promises,
        expose_params: Array.wrap(options.fetch(:exposes, [])).map(&:to_sym)
      })

      add_dependency(from: jobs.last.name.to_s, to: node.name.to_s) if jobs.any?
      jobs << node

      node.name
    end

    def compose(klass, options = {})
      params = options.fetch(:params, {})
      promises = Array.wrap(options.fetch(:requires, [])).each_with_object({}) do |attr_name, acc|
        acc[attr_name.to_sym] = PromiseAttribute.new(attr_name)
      end

      composition = klass.new(**params.merge!(promises))
      composition.parent = self
      children << composition

      composition.jobs.each do |job|
        job.incoming = []
        job.outgoing = []
        add_dependency(from: jobs.last.name.to_s, to: job.name.to_s) if jobs.any?
        jobs << job
      end

      composition.jobs.map(&:name)
    end

    def workflow(options = {}, &block)
      return unless block_given?

      klass = Class.new(Workflow) do
        define_method(:configure) do |**kwargs|
          instance_eval &block
        end
      end

      compose klass, options
    end

    def resolve_scope_promises!
      return scope if parent.nil?

      scope.fetch(:promises, {}).each do |attr_name, promise|
        unless promise.is_a? PromiseAttribute
          fail AttributeNotAPromise, "One or more workflow attributes should be a promise;" +
            "attribute: #{attr_name}, workflow: #{self.class}, workflow_id: #{id}"
        end

        if parent.scope[attr_name]
          scope[attr_name] = parent.scope[attr_name]
          scope[:promises].delete attr_name
        end
      end

      scope
    end

    def reload
      self.class.find(id)
    end

    def initial_jobs
      jobs.select(&:has_no_dependencies?)
    end

    def current_jobs
      initial_jobs.flat_map { |job| find_first_pending_jobs(job) }
    end

    def status
      case
        when failed?
          :failed
        when running?
          :running
        when finished?
          :finished
        when stopped?
          :stopped
        when enqueued?
          :enqueued
        else
          :running
      end
    end

    def started_at
      first_job ? first_job.started_at : nil
    end

    def finished_at
      last_job ? last_job.finished_at : nil
    end

    def to_hash
      name = self.class.to_s
      {
        name: name,
        id: id,
        arguments: @arguments,
        total: jobs.count,
        children_ids: children.map(&:id),
        scope: scope,
        finished: jobs.count(&:finished?),
        klass: name,
        jobs: jobs.map(&:as_json),
        status: status,
        stopped: stopped,
        started_at: started_at,
        finished_at: finished_at,
        enqueued_at: enqueued_at
      }
    end

    def to_json(options = {})
      Flush::JSON.encode(to_hash)
    end

    def self.descendants
      ObjectSpace.each_object(Class).select { |klass| klass < self }
    end

    def id
      @id ||= client.next_free_workflow_id
    end

    def children
      @children ||= []
    end

    def client
      @client ||= Client.new
    end

    private

    def setup
      configure(*@arguments)
      resolve_dependencies
    end

    def first_job
      jobs.min_by{ |n| n.started_at || Time.now.to_i }
    end

    def last_job
      jobs.max_by{ |n| n.finished_at || 0 } if finished?
    end

    def add_dependency(from:, to:)
      @dependencies << { from: from, to: to }
    end

    def find_first_pending_jobs(initial_job)
      return [initial_job] if !initial_job.succeeded?

      job_names = initial_job.outgoing
      jobs = job_names.map { |job_name| find_job(job_name) }
      jobs.flat_map do |job|
        find_first_pending_jobs(job)
      end
    end

    def current_timestamp
      Time.now.to_i
    end
  end
end
