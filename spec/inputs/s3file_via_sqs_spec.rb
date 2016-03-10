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
require "SecureRandom"
describe LogStash::Inputs::S3File_Via_Sqs do
  let(:queue_name) { "the-infinite-pandora-box" }
  let(:queue_url) { "https://sqs.test.local/#{queue_name}" }
  let(:temporary_directory) { Stud::Temporary.pathname }

  let(:config) { {"queue" => queue_name, "temporary_directory" => temporary_directory } }
  let(:mock_sqs) { Aws::SQS::Client.new({ :stub_responses => true }) }
  let(:mock_s3)  { Aws::S3::Client.new({ :stub_responses => true })  }
  let(:mock_sts) { Aws::STS::Client.new({ :stub_responses => true }) }

  let(:input) { LogStash::Inputs::S3File_Via_Sqs.new(config) }
  subject { input }


  context "valid credentials" do
    let(:queue) { [] }

    it "doesn't raise an error with valid credentials" do
      expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
      expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name }).and_return({:queue_url => queue_url })
      expect { subject.register }.not_to raise_error
    end

    context '.get_temp_credentials' do

      let(:assume_role_arn) { "arn:aws:iam::123456789012:role/AuthorizedRole" }
      let(:region) { 'us-east-1' }

      xit 'Assume Role Succesfully with an authorized role' do
        expect(Aws::STS::Client).to receive(:new).and_return(mock_sts)

        allow(Aws::AssumeRoleCredentials).to receive(:new).with({
          client: mock_sts,
          role_arn: assume_role_arn,
          role_session_name: "#{SecureRandom.hex}"
        }).and_return(mock_sts)

        credentials_config = {credentials: mock_sts, region: region}
        expect(Aws::S3::Client).to receive(:new).with(credentials_config).and_return(mock_s3)
        expect(Aws::SQS::Client).to receive(:new).with(credentials_config).and_return(mock_sqs)

        expect(mock_sqs).to receive(:get_queue_url).with({ :queue_name => queue_name,  queue_owner_aws_account_id: '123456789012' }).and_return({:queue_url => queue_url })
        expect { subject.register }.not_to raise_error
        expect { subject.get_temp_credentials }.not_to raise_error
      end
    end

    context "Parse SQS Messages" do
      let(:encoded_message) { File.read(File.expand_path( File.join(File.dirname(__FILE__), '..',  'fixtures', 'sqs_message.json')))  }
      let(:bad_encoded_message) { File.read(File.expand_path( File.join(File.dirname(__FILE__), '..',  'fixtures', 'bad_sqs_message.json')))  }
      let(:non_s3_encoded_message) { File.read(File.expand_path( File.join(File.dirname(__FILE__), '..',  'fixtures', 'non_s3_sqs_message.json')))  }

      before do
        allow(encoded_message).to receive(:body).and_return(encoded_message)
        allow(bad_encoded_message).to receive(:body).and_return(bad_encoded_message)
        allow(non_s3_encoded_message).to receive(:body).and_return(non_s3_encoded_message)
      end

      it "extracts the bucket and key from the sqs message" do
        expect(subject.parse_sqs(encoded_message)).to match_array([
          { bucket:'first-s3-bucket',  key: 'us-east-1/some/other/folder/first-log-file.gz'  },
          { bucket:'second-s3-bucket', key: 'us-east-1/some/other/folder/second-log-file.gz' }
        ])
      end

      it 'skip parsing sqs message with no key of Records in it' do
        expect(subject.parse_sqs(non_s3_encoded_message)).to be_empty
      end

      it 'will not crash when JSON is invalid' do
        expect(subject.parse_sqs(bad_encoded_message)).to be_empty
      end
    end

    context 'Given an array of s3 bucket names and keys each - .process_log' do
      let(:s3_file_info) { { bucket:'first-s3-bucket',  key: 'us-east-1/some/other/folder/first-log-file.gz'  } }
      let(:local_filename) { "#{temporary_directory}/first-log-file.gz" }
      before do
        expect { subject.setup_tmp_dir }.to change { Dir.exist?(temporary_directory) }.from(false).to(true)
        expect(Aws::S3::Client).to receive(:new).and_return(mock_s3)
        expect(Aws::SQS::Client).to receive(:new).and_return(mock_sqs)
        expect { subject.register }.not_to raise_error
      end

      it 'download log file from S3 to local disk' do
        expect( subject.download_remote_file(s3_file_info[:bucket], s3_file_info[:key], local_filename) ).to be(true)
      end

      let(:uncompressed_log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'uncompressed.log') }
      let(:compressed_log_file) { File.join(File.dirname(__FILE__), '..', 'fixtures', 'compressed.log.gz') }

      context 'compressed_log_file' do

        before do
         expect( subject.process_local_log(queue, compressed_log_file, s3_file_info) ).to be(true)
        end

        it '.process_local_log => process compressed log file and verfied logstash event queue with the correct number of events' do
          expect( queue.size ).to eq(2)
          expect( queue.clear).to be_empty
        end

        it ".deletes the temporary file" do
         expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
        end
      end

      context 'uncompressed_log_file' do
        let(:queue) { [] }

        before do
          expect( subject.process_local_log(queue, uncompressed_log_file, s3_file_info) ).to be(true)
        end

        it '.process_local_log => process uncompressed log file and verfied logstash event queue with the correct number of events' do
          expect( queue.size ).to eq(2)
        end

        it ".deletes the temporary file" do
         expect(Dir.glob(File.join(temporary_directory, "*")).size).to eq(0)
        end
      end
  end
end

end