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
  "fmt"
  . "github.com/mozilla-services/heka/pipeline"
  "github.com/crowdmob/goamz/sqs"
  "sync"
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
    decoder Decoder
    pack    *PipelinePack
    ok      bool
    resp    *sqs.ReceiveMessageResponse
  )

  if s.conf.Decoder != "" {
    if decoder, ok = h.PipelineConfig().Decoder(s.conf.Decoder); !ok {
      return fmt.Errorf("Decoder not found: %s", s.conf.Decoder)
    }
  }

  packSupply := ir.InChan()

  // create a wait group
  var wg sync.WaitGroup

  for s.running {
    // ir.LogMessage("Getting messages from SQS...")
    resp, err = s.queue.ReceiveMessage(10)
    if err != nil {
      ir.LogError(fmt.Errorf("Could not receive messages: %v", err))
    }

    for _, message := range resp.Messages {
      pack = <-packSupply

      wg.Add(1)
      go s.processMessage(&message, ir, h, pack, decoder, &wg)
    }

    // wait for each goroutine to exit
    wg.Wait()
  }

  return
}

func (s *SqsInput) processMessage(message *sqs.Message, ir InputRunner, h PluginHelper, pack *PipelinePack, decoder Decoder, wg  *sync.WaitGroup) {
  var (
    packs   []*PipelinePack
    err     error
  )

  data, err := base64.StdEncoding.DecodeString(message.Body)
  if err != nil {
    ir.LogError(fmt.Errorf("Could not decode message: %v", err))
  }

  pack.MsgBytes = data

  packs, err = decoder.Decode(pack)
  if packs != nil {
    for _, p := range packs {
      // ir.LogMessage("Injecting pack...")
      ir.Inject(p)
    }
  } else {
    if err != nil {
      ir.LogError(fmt.Errorf("Couldn't parse SQS message: %s - %v", message.Body, err))
    }
  }

  // ir.LogMessage("Deleting message from SQS...")
  _, err = s.queue.DeleteMessage(message)
  if err != nil {
    ir.LogError(fmt.Errorf("Could not delete message: %v - %v", message, err))
  }

  // ir.LogMessage("Done.")
  wg.Done()
}

func (s *SqsInput) Stop() {
  s.running = false
}

func init() {
  RegisterPlugin("SqsInput", func() interface{} {
    return new(SqsInput)
  })
}
