# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/S3FileViaSqs"
require "logstash/errors"
require "logstash/event"
require "logstash/json"
require_relative "../support/helpers"
require "stud/temporary"
require "aws-sdk"
require "fileutils"
require "ostruct"


describe LogStash::Inputs::S3FileViaSqs do
  let(:queue_name) { "the-infinite-pandora-box" }
  let(:queue_url) { "https://sqs.test.local/#{queue_name}" }

  let(:temporary_directory) { Stud::Temporary.pathname }
  let(:sincedb_path) { Stud::Temporary.pathname }
  let(:day) { 3600 * 24 }
  let(:config) {
    {
      "queue" => queue_name,
      "temporary_directory" => temporary_directory,
      "sincedb_path" => File.join(sincedb_path, ".sincedb")
    }
  }

  before do
    FileUtils.mkdir_p(sincedb_path)
    Thread.abort_on_exception = true
  end

  let(:input) { LogStash::Inputs::S3FileViaSqs.new(config) }

  subject { input }

  let(:mock_sqs) { Aws::SQS::Client.new({ :stub_responses => true }) }
  let(:mock_s3)  { Aws::S3::Client.new({ :stub_responses => true }) }
  let(:mock_sts) { Aws::STS::Client.new({ :stub_responses => true }) }
  let(:assume_role_arn) { "arn:aws:iam::123456789012:role/Logstash-CrossAccount-Role" }

  context "Normal account access (no assume role) - with invalid credentials" do
    before do
      expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
      expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name}) { raise Aws::SQS::Errors::ServiceError.new("bad-something", "bad token") }
    end


    it "raises a Configuration error if the credentials are bad" do
      expect { subject.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "Normal account access (no assume role) - valid credentials" do
    let(:queue) { [] }
    it "doesn't raise an error with valid credentials" do
      expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
      expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }).and_return({:queue_url => queue_url })
      expect { subject.register }.not_to raise_error
    end
  end


  describe "#setup_tmp_dir" do
    subject { LogStash::Inputs::S3FileViaSqs.new(config) }

    context "create temporary directory" do
      let(:temporary_directory) { Stud::Temporary.pathname }

      it "creates the direct when it doesn't exist" do
        expect { subject.setup_tmp_dir }.to change { Dir.exist?(temporary_directory) }.from(false).to(true)
      end
    end
  end

  describe '#get_temp_credentials' do
    subject { LogStash::Inputs::S3FileViaSqs.new(config) }

    before do
      expect(Aws::STS::Client).to receive(:new).and_return(mock_sts)
    end

    context 'invalid credentials' do
      it "raises a Configuration error if the credentials are bad" do
        expect { subject.send(:get_temp_credentials) }.to raise_error(LogStash::ConfigurationError)
      end
    end

    # context 'allow temporary credentials' do
    #   let(:role_credentials) { Aws::AssumeRoleCredentials.new({
    #       client: mock_sts,
    #       role_arn: "arn:aws:iam::123456789012:role/Logstash-CrossAccount-Role",
    #       role_session_name: "Session1"
    #   })

    #    }
    #   it "doesn't raise an error with valid credentials" do

    #     expect(Aws::SQS::Client).to receive(:new).with(role_credentials).and_return(mock_sqs)
    #     expect { subject.send(:get_temp_credentials)  }.not_to raise_error
    #   end
    # end
  end

  shared_examples "generated events"  do
    it 'should process events' do
      events = fetch_events(config)
      expect(events.size).to eq(2)
    end

    it "deletes the temporary file" do
      events = fetch_events(config)
      expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
    end
  end

  # context 'when working with logs' do
  #   let(:log) { double(:key => 'uncompressed.log', :last_modified => Time.now - 2 * day) }

  #   before do
  #     expect(log).to receive(:read)  { |&block| block.call(File.read(log_file)) }
  #   end

  #   # context "when event doesn't have a `message` field" do
  #   #   let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'json.log') }
  #   #   let(:config) {
  #   #     {
  #   #       "access_key_id" => "1234",
  #   #       "secret_access_key" => "secret",
  #   #       "bucket" => "logstash-test",
  #   #       "codec" => "json",
  #   #     }
  #   #   }

  #   #   include_examples "generated events"
  #   # end

  #   context 'compressed' do
  #     let(:log) { double(:key => 'log.gz', :last_modified => Time.now - 2 * day) }
  #     let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gz') }

  #     include_examples "generated events"
  #   end

  #   # context 'plain text' do
  #   #   let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }

  #   #   include_examples "generated events"
  #   # end

  #   # context 'encoded' do
  #   #   let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'invalid_utf8.log') }

  #   #   include_examples "generated events"
  #   # end

  #   # context 'cloudfront' do
  #   #   let(:log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'cloudfront.log') }

  #   #   it 'should extract metadata from cloudfront log' do
  #   #     events = fetch_events(config)

  #   #     events.each do |event|
  #   #       expect(event['cloudfront_fields']).to eq('date time x-edge-location c-ip x-event sc-bytes x-cf-status x-cf-client-id cs-uri-stem cs-uri-query c-referrer x-page-url​  c-user-agent x-sname x-sname-query x-file-ext x-sid')
  #   #       expect(event['cloudfront_version']).to eq('1.0')
  #   #     end
  #   #   end

  #   #   include_examples "generated events"
  #   # end
  # end


  # context "Get temporary credentials for cross account access with valid credentials" do
  #   let(:queue) { [] }
  #   let(:role_credentials) { Aws::AssumeRoleCredentials.new({client: mock_sts, role_arn: assume_role_arn,  role_session_name: "Session1"}) }

  #   before do
  #      expect(Aws::STS::Client).to receive(:new).and_return(mock_sts)
  #   end

  #   it "should not raise an error" do
  #     expect { subject.send(:get_temp_credentials) }.not_to raise_error
  #     #expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }).and_return({:queue_url => queue_url })
  #   end
  # end

  # context "when interrupting the plugin" do
  #     before do
  #       expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
  #       expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }).and_return({:queue_url => queue_url })
  #       expect(subject).to receive(:poller).and_return(mock_sqs).at_least(:once)

  #       # We have to make sure we create a bunch of events
  #       # so we actually really try to stop the plugin.
  #       #
  #       # rspec's `and_yield` allow you to define a fix amount of possible
  #       # yielded values and doesn't allow you to create infinite loop.
  #       # And since we are actually creating thread we need to make sure
  #       # we have enough work to keep the thread working until we kill it..
  #       #
  #       # I haven't found a way to make it rspec friendly
  #       mock_sqs.instance_eval do
  #         def poll(polling_options = {})
  #           loop do
  #             yield [OpenStruct.new(:body => LogStash::Json::dump({ "message" => "hello world"}))], OpenStruct.new
  #           end
  #         end
  #       end
  #     end

  #     it_behaves_like "an interruptible input plugin"
  #   end


end