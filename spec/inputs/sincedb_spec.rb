# # encoding: utf-8
# require "logstash/devutils/rspec/spec_helper"
# require "logstash/inputs/s3file_via_sqs"
# require "stud/temporary"
# require "fileutils"

# describe LogStash::Inputs::S3File_Via_Sqs::SinceDB::File do
#   let(:file) { Stud::Temporary.file.path }
#   subject { LogStash::Inputs::S3File_Via_Sqs::SinceDB::File.new(file) }
#   before do
#     FileUtils.touch(file)
#   end

#   it "doesnt raise an exception if the file is empty" do
#     expect { subject.read }.not_to raise_error
#   end
# end
