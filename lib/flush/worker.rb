require 'sidekiq'

module Flush
  class Worker
    include ::Sidekiq::Worker
    sidekiq_options retry: false

    def perform(workflow_id, job_id, retrying = false)
      setup_job(workflow_id, job_id)

      start = Time.now
      report(:started, start)

      failed = false
      error = nil

      mark_as_started
      job.mark_as_retried if retrying

      if job.should_run?
        job.run
        job.mark_as_performed
      else
        job.mark_as_skipped
      end

      wait_until_job_is_done!
      update_workflow_scope
      mark_as_finished

      report(:finished, start)

      if workflow.finished?
        client.finish_workflow(workflow)
      else
        enqueue_outgoing_jobs
      end
    rescue Exception => error
      update_workflow_scope
      mark_as_failed(error)
      report(:failed, start, error.message)
      raise error
    end

    private
    attr_reader :client, :workflow, :job

    def client
      @client ||= Flush::Client.new
    end

    def setup_job(workflow_id, job_id)
      @workflow ||= client.find_workflow(workflow_id)
      @job ||= workflow.find_job(job_id)
    end

    def incoming_payloads
      payloads = {}
      job.incoming.each do |job_name|
       job = client.load_job(workflow.id, job_name)
       payloads[job.klass.to_s] ||= []
       payloads[job.klass.to_s] << {:id => job.name, :payload => job.output_payload}
      end
      payloads
    end

    def wait_until_job_is_done!
      loop do
        break if job.done?
        job.on_done_checking
        sleep 5
      end
    end

    def update_workflow_scope
      job.workflow.merge_scope(job.output_payload.symbolize_keys)
      client.persist_workflow(job.workflow)
    end

    def mark_as_finished
      job.finish!
      client.persist_job(workflow.id, job)
    end

    def mark_as_failed(error)
      job.fail!
      client.fail_workflow(workflow)

      job.workflow.merge_scope({ error: {
        class: error.class.to_s,
        message: error.message,
        backtrace: error.backtrace
      }})

      client.persist_job(workflow.id, job)
    end

    def mark_as_started
      job.start!
      client.persist_job(workflow.id, job)
    end

    def report_workflow_status
      client.workflow_report({
        workflow_id:  workflow.id,
        status:       workflow.status,
        started_at:   workflow.started_at,
        finished_at:  workflow.finished_at
      })
    end

    def report(status, start, error = nil)
      message = {
        status: status,
        workflow_id: workflow.id,
        job: job.name,
        duration: elapsed(start)
      }
      message[:error] = error if error
      client.worker_report(message)
    end

    def elapsed(start)
      (Time.now - start).to_f.round(3)
    end

    def enqueue_outgoing_jobs
      job.outgoing.each do |job_name|
        out = client.load_job(workflow.id, job_name)
        if out.ready_to_start?
          client.enqueue_job(workflow.id, out)
        end
      end
    end
  end
end
