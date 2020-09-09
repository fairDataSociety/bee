// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package recovery

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math/big"
	"time"

	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/pss"
	"github.com/ethersphere/bee/pkg/pushsync"
	"github.com/ethersphere/bee/pkg/soc"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/trojan"
)

const (
	// recoveryTopicText is the string used to construct the recovery topic.
	recoveryTopicText = "RECOVERY"
)

var (
	// RecoveryTopic is the topic used for repairing globally pinned chunks.
	RecoveryTopic         = trojan.NewTopic(RecoveryTopicText)
)

// RecoveryHook defines code to be executed upon failing to retrieve chunks.
type RecoveryHook func(chunkAddress swarm.Address, targets trojan.Targets, chunkC chan swarm.Chunk) (swarm.Address, error)

// sender is the function call for sending trojan chunks.
type PssSender interface {
	Send(ctx context.Context, targets trojan.Targets, topic trojan.Topic, payload []byte) error
}

// NewRecoveryHook returns a new RecoveryHook with the sender function defined.
func NewRecoveryHook(pss PssSender, overlay swarm.Address, privateKey *ecdsa.PrivateKey) RecoveryHook {
	return func(chunkAddress swarm.Address, targets trojan.Targets, chunkC chan swarm.Chunk) (swarm.Address, error) {
		socEnvelope, err := createSelfAddressedEnvelope(overlay, 1, chunkAddress, privateKey)
		if err != nil {
			return swarm.ZeroAddress, err
		}
		payload := append(socEnvelope.Address().Bytes(), socEnvelope.Data()...)
		ctx := context.Background()
		err = pss.Send(ctx, targets, RecoveryTopic, payload)
		r.chunkChanMap[chunkAddress.String()] = chunkC
		return err
	}
}


// NewRepairHandler creates a repair function to re-upload globally pinned chunks to the network with the given store.
func NewRepairHandler(s storage.Storer, logger logging.Logger, pushSyncer pushsync.PushSyncer) pss.Handler {
	return func(ctx context.Context, m *trojan.Message) {
		chAddr := m.Payload

		// check if the chunk exists in the local store and proceed.
		// otherwise the Get will trigger a unnecessary network retrieve
		addrToRetrieve := swarm.NewAddress(envelopePayloadId)
		exists, err := s.Has(ctx, addrToRetrieve)
		if err != nil {
			return
		}
		if !exists {
			return
		}

		// retrieve the chunk from the local store
		ch, err := s.Get(ctx, storage.ModeGetRequest, addrToRetrieve)
		if err != nil {
			logger.Tracef("chunk repair: error while getting chunk for repairing: %v", err)
			return
		}

		// push the chunk using push sync so that it reaches it destination in network
		_, err = pushSyncer.PushChunkToClosest(ctx, ch)
		if err != nil {
			logger.Tracef("chunk repair: error while sending chunk or receiving receipt: %v", err)
			return
		}
	}
}
