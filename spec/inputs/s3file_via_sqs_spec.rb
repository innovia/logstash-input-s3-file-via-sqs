# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/s3file_via_sqs"
require "logstash/errors"
require "logstash/event"
require "logstash/json"
require_relative "../support/helpers"
require "stud/temporary"
require "aws-sdk"
require "fileutils"
require "ostruct"

describe LogStash::Inputs::S3File_Via_Sqs do
  let(:queue_name) { "the-infinite-pandora-box" }
  let(:queue_url) { "https://sqs.test.local/#{queue_name}" }
  let(:queue) { [] }
  let(:config) { {"queue" => queue_name, "temporary_directory" => "/tmp/test" } }
  let(:mock_sqs) { Aws::SQS::Client.new({ :stub_responses => true }) }
  let(:mock_sts) { Aws::STS::Client.new({ :stub_responses => true }) }
  let(:decoded_message) { { "bonjour" => "awesome" }  }
  let(:encoded_message)  { double("sqs_message", :body => LogStash::Json::dump(decoded_message)) }

  let(:input) { LogStash::Inputs::S3File_Via_Sqs.new(config) }
  subject { input }


  context "receiving messages" do
      before do
        expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)
      end

      it "creates logstash event" do
        expect(mock_sqs).to receive(:poll).with(anything()).and_yield([encoded_message], double("stats"))
        subject.run(queue)
        expect(queue.pop["bonjour"]).to eq(decoded_message["bonjour"])
      end
  end

end