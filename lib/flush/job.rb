module Flush
  class Job
    attr_accessor :workflow_id, :incoming, :outgoing, :params,
      :finished_at, :failed_at, :started_at, :enqueued_at, :skipped_at,
      :retry_count, :payloads_hash, :klass, :workflow, :promises, :expose_params
    attr_reader :name, :output_payload, :params, :payloads
    attr_writer :output

    def self.from_hash(workflow, hash)
      promises = build_promises hash.fetch(:promises, {})
      hash[:promises] = promises if promises.any?
      job_params = hash.fetch(:params, {}).merge(promises)

      job_workflow = workflow.find_in_descendants(hash[:workflow_id])
      if job_workflow.nil?
        fail JobWorkflowNotFoundInDescendants, "Step workflow not found in the hierarchy; " +
          "job: #{name}, root_workflow_id: #{workflow.id}, seeked_workflow_id: #{hash[:workflow_id]}"
      end

      job = hash[:klass].constantize.new(**job_params.symbolize_keys)
      job.setup(job_workflow, hash)
      job.resolve_promises!
      job
    end

    def setup(workflow, opts)
      @workflow = workflow
      @klass = self.class.name
      @name = opts[:name]
      @promises = opts[:promises] || {}
      @incoming = opts[:incoming] || []
      @outgoing = opts[:outgoing] || []
      @failed_at = opts[:failed_at]
      @finished_at = opts[:finished_at]
      @started_at = opts[:started_at]
      @enqueued_at = opts[:enqueued_at]
      @skipped_at = opts[:enqueued_at]
      @retry_count = opts[:retry_count] || 0
      @params = opts[:params] || {}
      @expose_params = opts[:expose_params] || []

      self
    end

    def should_run?
      true
    end

    def run
      fail NotImplementedError
    end

    def done?
      true
    end

    def output
      @output ||= {}
    end

    def as_json
      {
        workflow_id: workflow.id,
        name: name,
        klass: self.class.to_s,
        incoming: incoming,
        outgoing: outgoing,
        finished_at: finished_at,
        enqueued_at: enqueued_at,
        started_at: started_at,
        failed_at: failed_at,
        skipped_at: skipped_at,
        retry_count: retry_count,
        params: params,
        promises: promises,
        expose_params: expose_params
      }
    end

    def to_json(options = {})
      Flush::JSON.encode(as_json)
    end

    def output
      @output ||= {}
    end

    def output=(output)
      @output = output
    end

    def resolve_promises!
      promises.each do |attr_name, promise|
        unless promise.is_a? PromiseAttribute
          fail AttributeNotAPromise, "One or more job attributes should be a promise;" +
            "attribute: #{attr_name}, workflow: #{workflow.class}, job: #{name}"
        end

        attr_name = promise.attr_name.to_sym

        next unless workflow.scope.key? attr_name.to_sym

        attribute = workflow.scope[attr_name.to_sym]
        public_send("#{attr_name}=", attribute)
        params[attr_name] = attribute
      end

      self
    end

    def payloads
      payload_h = {}
      payloads_hash.each {|k,val| payload_h[k.to_s] = val.map {|h| h[:payload] }}
      payload_h
    end

    def output_payload
      expose_params.each_with_object({}) do |attr_name, acc|
        attribute = output[attr_name.to_sym] || output[attr_name.to_s]

        if attribute.nil?
          fail ExposeParameterNotFoundInOutput, "The parameter that was suposed to be exposed in the workflow was " +
            "not found in the step output; attribute: #{attr_name}, workflow: #{workflow.class}, job: #{name}"
        end

        acc[attr_name] = attribute
      end
    end

    def start!
      @started_at = current_timestamp
      on_start
    end

    def enqueue!
      @enqueued_at = current_timestamp
      @started_at = nil
      @finished_at = nil
      @failed_at = nil
      @skipped_at = nil
      # TODO: validate when there is no promises in construction
      on_enqueue
    end

    def finish!
      @finished_at = current_timestamp
      on_success
    end

    def fail!
      @finished_at = @failed_at = current_timestamp
      on_fail
    end

    def mark_as_performed
      after_run
    end

    def mark_as_skipped
      @skipped_at = current_timestamp
      on_skipping
    end

    def mark_as_retried
      @retry_count = @retry_count.to_i + 1
      on_retry
    end

    def on_start
    end

    def on_enqueue
    end

    def on_success
    end

    def on_fail
    end

    def after_run
    end

    def on_skipping
    end

    def on_retry
    end

    def on_done_checking
    end

    def enqueued?
      !enqueued_at.nil?
    end

    def finished?
      !finished_at.nil?
    end

    def failed?
      !failed_at.nil?
    end

    def skipped?
      !skipped_at.nil?
    end

    def succeeded?
      finished? && !failed?
    end

    def started?
      !started_at.nil?
    end

    def running?
      started? && !finished?
    end

    def ready_to_start?
      !running? && !enqueued? && !finished? && !failed? && parents_succeeded?
    end

    def parents_succeeded?
      incoming.all? do |name|
        @workflow.find_job(name).succeeded?
      end
    end

    def has_no_dependencies?
      incoming.empty?
    end

    def workflow_id
      workflow.id
    end

    private

    def logger
      Sidekiq.logger
    end

    def current_timestamp
      Time.now.to_i
    end

    def self.build_promises(promises)
      promises.each_with_object({}) do |(attr_name, params), acc|
        acc[attr_name] = PromiseAttribute.new(attr_name)
      end
    end
  end
end
