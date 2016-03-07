# Logstash S3 Via SQS Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

Pull S3 file names from SQS, Download the files, parse it with Logstash then delete the local file


## How to work with this plugin
[Configure event notification on S3 bucket for put operations to send the newly added file to SQS designated queue](https://aws.amazon.com/blogs/aws/s3-event-notification)

## Required S3 Permissions
This plugin reads from your S3 bucket, and would require the following
permissions applied to the AWS IAM Policy being used:

* `s3:ListBucket` to check if the S3 bucket exists and list objects in it.
* `s3:GetObject` to check object metadata and download objects from S3 buckets.

## Requires SQS Permissions
The "consumer" identity must have the following permissions on the queue:

  * `sqs:ChangeMessageVisibility`
  * `sqs:ChangeMessageVisibilityBatch`
  * `sqs:DeleteMessage`
  * `sqs:DeleteMessageBatch`
  * `sqs:GetQueueAttributes`
  * `sqs:GetQueueUrl`
  * `sqs:ListQueues`
  * `sqs:ReceiveMessage``

 Typically, you should setup an IAM policy, create a user and apply the IAM policy to the user.
 A sample policy is as follows:
````json
     {
       "Statement": [
         {
           "Action": [
             "sqs:ChangeMessageVisibility",
             "sqs:ChangeMessageVisibilityBatch",
             "sqs:GetQueueAttributes",
             "sqs:GetQueueUrl",
             "sqs:ListQueues",
             "sqs:SendMessage",
             "sqs:SendMessageBatch"
           ],
           "Effect": "Allow",
           "Resource": [
             "arn:aws:sqs:us-east-1:123456789012:Logstash"
           ]
         }
       ]
     }
````
[This plugin supports cross account access with assume role](https://blogs.aws.amazon.com/security/post/Tx70F69I9G8TYG/How-to-enable-cross-account-access-to-the-AWS-Management-Console)
## Requires STS Permissions
```json
"AssumeRolePolicyDocument" : {
    "Version" : "2012-10-17",
    "Statement" : [ {
      "Effect" : "Allow",
      "Principal" : {
        "Service" : [ "ec2.amazonaws.com" ]
      },
      "Action" : [ "sts:AssumeRole" ]
    } ]
}
```

## Installing
clone the repository
gem build logstash-input-s3file-via-sqs.gemspec
./bin/plugin install logstash-input-s3file-via-sqs-1.0.0.gem

## Plugin Configuration Options
```ruby
input {
    s3file_via_sqs {
        queue => "LogsSomeQueue"
        assume_role_arn => "arn:aws:iam::123456789012:role/Logstash-CrossAccount-Role"
        aws_queue_owner_id => "123456789012"
        temporary_directory => "/some/temporary/download/path"
    }
}

```

 * `polling_frequency - how often to pull from SQS (number) in seconds`
 * `assume_role_arn - the full cross acount IAM roll arn like "arn:aws:iam::123456789012:role/Logstash-CrossAccount-Role"`
 * `queue - queue name`
 * `aws_queue_owner_id - the aws account id like 123456789012`
 * `temporary_directory - where to save the downloaded files`


## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and maintainers or community members  saying "send patches or die" - you will not see that here.

It is more important to the community that you are able to contribute.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.