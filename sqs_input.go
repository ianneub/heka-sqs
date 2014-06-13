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
  // "errors"
  "fmt"
  . "github.com/mozilla-services/heka/pipeline"
  "github.com/crowdmob/goamz/sqs"
)

type SqsInput struct {
  conf         *SqsInputConfig
  decoder      Decoder
  queue        *sqs.Queue
  running      bool
}

type SqsInputConfig struct {
  // AWS access key id
  AccessKey string `toml:"access_key"`
  // AWS secret access key
  SecretKey string `toml:"secret_key"`
  // Queue name
  Queue string
  // Encoder to use
  Decoder string
  // AWS Region to connect to
  Region string
}

func (s *SqsInput) ConfigStruct() interface{} {
  return &SqsInputConfig{
    Decoder: "ProtobufDecoder",
    Region: "us.east",
  }
}

func (s *SqsInput) Init(config interface{}) (err error) {
  s.running = true
  s.conf = config.(*SqsInputConfig)

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

func (s *SqsInput) Run(ir InputRunner, h PluginHelper) (err error) {
  var (
    dRunner DecoderRunner
    decoder Decoder
    pack    *PipelinePack
    e       error
    ok      bool
    resp    *sqs.ReceiveMessageResponse
    packs   []*PipelinePack
  )

  if s.conf.Decoder != "" {
    if dRunner, ok = h.DecoderRunner(s.conf.Decoder, fmt.Sprintf("%s-%s", ir.Name(), s.conf.Decoder)); !ok {
      return fmt.Errorf("Decoder not found: %s", s.conf.Decoder)
    }
    decoder = dRunner.Decoder()
    fmt.Printf("Decoder found was: %v", decoder)
  }

  packSupply := ir.InChan()

  for s.running {
    resp, err = s.queue.ReceiveMessage(10)
    if err != nil {
      ir.LogError(fmt.Errorf("Could not receive messages: %v", err))
    }

    for _, message := range resp.Messages {
      data, err := base64.StdEncoding.DecodeString(message.Body)
      if err != nil {
        ir.LogError(fmt.Errorf("Could not decode message: %v", err))   
      }

      pack = <-packSupply
      pack.MsgBytes = data

      packs, e = decoder.Decode(pack)
      if packs != nil {
        for _, p := range packs {
          ir.Inject(p)
        }
      } else {
        if e != nil {
          ir.LogError(fmt.Errorf("Couldn't parse SQS message: %s", message.Body))
        }
      }

      pack.Recycle()
    }
  }
  return
}

func (s *SqsInput) Stop() {
  s.running = false
}

func init() {
  RegisterPlugin("SqsInput", func() interface{} {
    return new(SqsInput)
  })
}
