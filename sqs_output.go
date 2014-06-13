/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2012
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Ian Neubert (ian@ianneubert.com)
#
# ***** END LICENSE BLOCK *****/

package sqs

import (
  "encoding/base64"
  "errors"
  "fmt"
  . "github.com/mozilla-services/heka/pipeline"
  "github.com/crowdmob/goamz/sqs"
)

type SqsOutput struct {
  conf         *SqsOutputConfig
  encoder      Encoder
  queue        *sqs.Queue
}

type SqsOutputConfig struct {
  // AWS access key id
  AccessKey string `toml:"access_key"`
  // AWS secret access key
  SecretKey string `toml:"secret_key"`
  // Queue name
  Queue string
  // Encoder to use
  Encoder string
  // AWS Region to connect to
  Region string
}

func (s *SqsOutput) ConfigStruct() interface{} {
  return &SqsOutputConfig{
    Encoder: "ProtobufEncoder",
    Region: "us.east",
  }
}

func (s *SqsOutput) Init(config interface{}) (err error) {
  s.conf = config.(*SqsOutputConfig)

  if s.conf.AccessKey == "" {
    return fmt.Errorf("AccessKey must be set")
  }

  if s.conf.SecretKey == "" {
    return fmt.Errorf("SecretKey must be set")
  }

  if s.conf.Queue == "" {
    return fmt.Errorf("Queue must be set")
  }

  // create sqs client
  client, err := sqs.NewFrom(s.conf.AccessKey, s.conf.SecretKey, s.conf.Region)
  if err != nil {
    return fmt.Errorf("Could not create SQS client: %v", err)
  }

  // get the SQS queue
  s.queue, err = client.GetQueue(s.conf.Queue)
  if err != nil {
    return fmt.Errorf("Could not get SQS queue: %v", err)
  }
  return
}

func (s *SqsOutput) Run(or OutputRunner, h PluginHelper) (err error) {
  var (
    pack     *PipelinePack
    // persist  uint8
    // ok       bool = true
    outBytes []byte
    inChan chan *PipelinePack = or.InChan()
  )

  s.encoder = or.Encoder()
  if s.encoder == nil {
    return errors.New("Encoder required.")
  }

  for pack = range inChan {
    if outBytes, err = s.encoder.Encode(pack); err != nil {
      or.LogError(fmt.Errorf("Error encoding message: %s", err))
      pack.Recycle()
      continue
    }

    // send the message to the queue
    _, err = s.queue.SendMessage(base64.StdEncoding.EncodeToString(outBytes))
    if err != nil {
      or.LogError(fmt.Errorf("Could not send SQS message: %v", err))
      pack.Recycle()
      continue
    }

    pack.Recycle()
  }
  return
}

func init() {
  RegisterPlugin("SqsOutput", func() interface{} {
    return new(SqsOutput)
  })
}
