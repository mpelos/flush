require 'spec_helper'

describe "Workflows" do
  context "when all jobs finish successfuly" do
    it "marks workflow as completed" do
      flow = TestWorkflow.create
      flow.start!

      Flush::Worker.drain

      flow = flow.reload
      expect(flow).to be_finished
      expect(flow).to_not be_failed
    end
  end

  it "runs the whole workflow in proper order" do
    flow = TestWorkflow.create
    flow.start!

    expect(Flush::Worker).to have_jobs(flow.id, jobs_with_id(['Prepare']))

    Flush::Worker.perform_one
    expect(Flush::Worker).to have_jobs(flow.id, jobs_with_id(["FetchFirstJob", "FetchSecondJob"]))

    Flush::Worker.perform_one
    expect(Flush::Worker).to have_jobs(flow.id, jobs_with_id(["FetchSecondJob", "PersistFirstJob"]))

    Flush::Worker.perform_one
    expect(Flush::Worker).to have_jobs(flow.id, jobs_with_id(["PersistFirstJob"]))

    Flush::Worker.perform_one
    expect(Flush::Worker).to have_jobs(flow.id, jobs_with_id(["NormalizeJob"]))

    Flush::Worker.perform_one

    expect(Flush::Worker.jobs).to be_empty
  end

  it "passes payloads down the workflow" do
    class UpcaseJob < Flush::Job
      def work
        output params[:input].upcase
      end
    end

    class PrefixJob < Flush::Job
      def work
        output params[:prefix].capitalize
      end
    end

    class PrependJob < Flush::Job
      def work
        string = "#{payloads['PrefixJob'].first}: #{payloads['UpcaseJob'].first}"
        output string
      end
    end

    class PayloadWorkflow < Flush::Workflow
      def configure
        run UpcaseJob, params: {input: "some text"}
        run PrefixJob, params: {prefix: "a prefix"}
        run PrependJob, after: [UpcaseJob, PrefixJob]
      end
    end

    flow = PayloadWorkflow.create
    flow.start!

    Flush::Worker.perform_one
    expect(flow.reload.find_job("UpcaseJob").output_payload).to eq("SOME TEXT")

    Flush::Worker.perform_one
    expect(flow.reload.find_job("PrefixJob").output_payload).to eq("A prefix")

    Flush::Worker.perform_one
    expect(flow.reload.find_job("PrependJob").output_payload).to eq("A prefix: SOME TEXT")


  end

  it "passes payloads from workflow that runs multiple same class jobs with nameized payloads" do
    class RepetitiveJob < Flush::Job
      def work
        output params[:input]
      end
    end

    class SummaryJob < Flush::Job
      def work
        output payloads['RepetitiveJob']
      end
    end

    class PayloadWorkflow < Flush::Workflow
      def configure
        jobs = []
        jobs << run(RepetitiveJob, params: {input: 'first'})
        jobs << run(RepetitiveJob, params: {input: 'second'})
        jobs << run(RepetitiveJob, params: {input: 'third'})
        run SummaryJob, after: jobs
      end
    end

    flow = PayloadWorkflow.create
    flow.start!

    Flush::Worker.perform_one
    expect(flow.reload.find_job(flow.jobs[0].name).output_payload).to eq('first')

    Flush::Worker.perform_one
    expect(flow.reload.find_job(flow.jobs[1].name).output_payload).to eq('second')

    Flush::Worker.perform_one
    expect(flow.reload.find_job(flow.jobs[2].name).output_payload).to eq('third')

    Flush::Worker.perform_one
    expect(flow.reload.find_job(flow.jobs[3].name).output_payload).to eq(%w(first second third))

  end
end
