# Heka SQS Input/Output plugin

This is an input and output plugin that enables sending messages to and receiving messages from [SQS](http://aws.amazon.com/sqs/) in [Heka](https://github.com/mozilla-services/heka).

## How to use

To use either plugin you will need to add this project to your Heka source code by adding a line to `cmake/plugin_loader.cmake` that will load the plugin, like this:

    add_external_plugin(git https://github.com/ianneub/heka-sqs master)


### SqsInput

Example configuration:

    [SqsInput]
    access_key = ""
    secret_key = ""
    queue = "my_queue"

### SqsOutput

You must use a specially configured `ProtobufEncoder` that has `include_framing` set to `false` in order to use the `SqsOutput` plugin. You must create a custom `ProtobufEncoder` because the default encoder sets the framing to true, which will not work with this plugin.

Example configuration:

    [SqsOutput]
    access_key = ""
    secret_key = ""
    queue = "my_queue"
    message_matcher = "TRUE"
    encoder = "protobuf-noframe"
    
    [protobuf-noframe]
    type = "ProtobufEncoder"
    include_framing = false
    
## Questions

Please create an issue on GitHub with any questions or comments. Pull requests are especially appreciated.

## License

See `LICENSE.txt`.
