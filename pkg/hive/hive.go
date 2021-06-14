// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package hive exposes the hive protocol implementation
// which is the discovery protocol used to inform and be
// informed about other peers in the network. It gossips
// about all peers by default and performs no specific
// prioritization about which peers are gossipped to
// others.
package hive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/addressbook"
	"github.com/ethersphere/bee/pkg/bzz"
	"github.com/ethersphere/bee/pkg/hive/pb"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/p2p"
	"github.com/ethersphere/bee/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/pkg/swarm"
	ma "github.com/multiformats/go-multiaddr"

	"golang.org/x/time/rate"
)

const (
	protocolName    = "hive"
	protocolVersion = "1.0.0"
	peersStreamName = "peers"
	messageTimeout  = 1 * time.Minute // maximum allowed time for a message to be read or written.
	maxBatchSize    = 30
)

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
	limitBurst           = 4 * int(swarm.MaxBins)
	limitRate            = rate.Every(time.Minute)
)

type Service struct {
	streamer        p2p.Streamer
	addressBook     addressbook.GetPutter
	addPeersHandler func(...swarm.Address)
	networkID       uint64
	logger          logging.Logger
	metrics         metrics
	limiter         map[string]*rate.Limiter
	limiterLock     sync.Mutex
	sqlDB           *sql.DB
}

func New(streamer p2p.Streamer, addressbook addressbook.GetPutter, networkID uint64, sqlDB *sql.DB, logger logging.Logger) *Service {
	return &Service{
		streamer:    streamer,
		logger:      logger,
		addressBook: addressbook,
		networkID:   networkID,
		metrics:     newMetrics(),
		limiter:     make(map[string]*rate.Limiter),
		sqlDB:       sqlDB,
	}
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    peersStreamName,
				Handler: s.peersHandler,
			},
		},
		DisconnectIn:  s.disconnect,
		DisconnectOut: s.disconnect,
	}
}

func (s *Service) BroadcastPeers(ctx context.Context, addressee swarm.Address, peers ...swarm.Address) error {
	max := maxBatchSize
	s.metrics.BroadcastPeers.Inc()
	s.metrics.BroadcastPeersPeers.Add(float64(len(peers)))

	for len(peers) > 0 {
		if max > len(peers) {
			max = len(peers)
		}
		if err := s.sendPeers(ctx, addressee, peers[:max]); err != nil {
			return err
		}

		peers = peers[max:]
	}

	return nil
}

func (s *Service) SetAddPeersHandler(h func(addr ...swarm.Address)) {
	s.addPeersHandler = h
}

func (s *Service) sendPeers(ctx context.Context, peer swarm.Address, peers []swarm.Address) (err error) {
	s.metrics.BroadcastPeersSends.Inc()
	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, peersStreamName)
	if err != nil {
		return fmt.Errorf("new stream: %w", err)
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()
	//w, _ := protobuf.NewWriterAndReader(stream)
	var peersRequest pb.Peers
	for _, p := range peers {
		addr, err := s.addressBook.Get(p)
		if err != nil {
			if err == addressbook.ErrNotFound {
				s.logger.Debugf("hive broadcast peers: peer not found in the addressbook. Skipping peer %s", p)
				continue
			}
			return err
		}

		peersRequest.Peers = append(peersRequest.Peers, &pb.BzzAddress{
			Overlay:   addr.Overlay.Bytes(),
			Underlay:  addr.Underlay.Bytes(),
			Signature: addr.Signature,
		})
	}

	//if err := w.WriteMsgWithContext(ctx, &peersRequest); err != nil {
	//	return fmt.Errorf("write Peers message: %w", err)
	//}

	return nil
}

func (s *Service) peersHandler(ctx context.Context, peer p2p.Peer, stream p2p.Stream) error {
	s.metrics.PeersHandler.Inc()
	_, r := protobuf.NewWriterAndReader(stream)
	ctx, cancel := context.WithTimeout(ctx, messageTimeout)
	defer cancel()
	var peersReq pb.Peers
	if err := r.ReadMsgWithContext(ctx, &peersReq); err != nil {
		_ = stream.Reset()
		return fmt.Errorf("read requestPeers message: %w", err)
	}

	s.metrics.PeersHandlerPeers.Add(float64(len(peersReq.Peers)))

	if err := s.rateLimitPeer(peer.Address, len(peersReq.Peers)); err != nil {
		_ = stream.Reset()
		return err
	}

	// close the stream before processing in order to unblock the sending side
	// fullclose is called async because there is no need to wait for confirmation,
	// but we still want to handle not closed stream from the other side to avoid zombie stream
	go stream.FullClose()

	var peers []swarm.Address
	var bzzPeers []*bzz.Address
	count := 0
	for _, newPeer := range peersReq.Peers {
		bzzAddress, err := bzz.ParseAddress(newPeer.Underlay, newPeer.Overlay, newPeer.Signature, s.networkID)
		if err != nil {
			s.logger.Warningf("skipping peer in response %s: %v", newPeer.String(), err)
			continue
		}

		err = s.addressBook.Put(bzzAddress.Overlay, *bzzAddress)
		if err != nil {
			s.logger.Warningf("skipping peer in response %s: %v", newPeer.String(), err)
			continue
		}

		peers = append(peers, bzzAddress.Overlay)
		bzzPeers = append(bzzPeers, bzzAddress)
		count++
	}

	if s.addPeersHandler != nil {
		s.addPeersHandler(peers...)
	}

	err := s.updatePeerCount(peer.Address.String(), count)
	if err != nil {
		s.logger.Errorf("peersHandler: %v", err.Error())
		return err
	}
	err = s.addNeighboursAsPeers(bzzPeers)
	if err != nil {
		s.logger.Errorf("peersHandler: %v", err.Error())
		return err
	}

	return nil
}

func (s *Service) rateLimitPeer(peer swarm.Address, count int) error {

	s.limiterLock.Lock()
	defer s.limiterLock.Unlock()

	addr := peer.String()

	limiter, ok := s.limiter[addr]
	if !ok {
		limiter = rate.NewLimiter(limitRate, limitBurst)
		s.limiter[addr] = limiter
	}

	if limiter.AllowN(time.Now(), count) {
		return nil
	}

	return ErrRateLimitExceeded
}

func (s *Service) disconnect(peer p2p.Peer) error {
	s.limiterLock.Lock()
	defer s.limiterLock.Unlock()

	delete(s.limiter, peer.Address.String())

	return nil
}

func (s *Service) updatePeerCount(baseOverlay string, count int) error {
	tx, err := s.sqlDB.Begin()
	if err != nil {
		s.logger.Errorf("updatePeerCount: %v", err)
		return err
	}

	{
		updateStatement := `update PEER_INFO set PEERS_COUNT = PEERS_COUNT + ? WHERE OVERLAY = ?`
		statement, err := tx.Prepare(updateStatement)
		if err != nil {
			tx.Rollback()
			s.logger.Errorf("updatePeerCount: %v", err)
			return err
		}
		defer statement.Close()

		_, err = statement.Exec(count, baseOverlay)
		if err != nil {
			tx.Rollback()
			s.logger.Errorf("updatePeerCount: %v", err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		s.logger.Errorf("updatePeerCount: %v", err)
		return err
	}
	s.logger.Debugf("updatePeerCount: incremented peer count of %s by another %d", baseOverlay, count)
	return nil
}

func (s *Service) addNeighboursAsPeers(bzzPeers []*bzz.Address) error {
	for _, bzzPeer := range bzzPeers {
		rows, err := s.sqlDB.Query("select OVERLAY from PEER_INFO WHERE OVERLAY = ? ", bzzPeer.Overlay.String())
		if err != nil {
			s.logger.Errorf("addNeighboursAsPeers: %v", err)
			continue
		}
		defer rows.Close()

		overlayPresent := ""
		for rows.Next() {
			err := rows.Scan(&overlayPresent)
			if err != nil {
				s.logger.Errorf("addNeighboursAsPeers: %v", err)
				continue
			}
		}
		if overlayPresent == "" {
			tx, err := s.sqlDB.Begin()
			if err != nil {
				s.logger.Errorf("addNeighboursAsPeers: %v", err)
				continue
			}

			insertStatement := `insert into PEER_INFO (OVERLAY, IP4or6, IP, PROTOCOL, PORT, UNDERLAY,  CONNECTED, RETRY_COUNT, PEERS_COUNT) values (?, ?, ?, ?, ?, ?, ?, ?, ?)`
			statement, err := tx.Prepare(insertStatement)
			if err != nil {
				tx.Rollback()
				s.logger.Errorf("addNeighboursAsPeers: error preparing statement: %v", err)
				continue
			}
			defer statement.Close()

			overlay := bzzPeer.Overlay.String()
			underlay := bzzPeer.Underlay.String()
			cols := strings.Split(underlay, "/")
			if len(cols) != 7 && len(cols) != 6 {
				tx.Rollback()
				s.logger.Errorf("addNeighboursAsPeers: invalid underlay format %s", underlay)
				continue
			}

			var addresses []ma.Multiaddr
			if len(cols) == 6 && strings.HasPrefix(cols[1], "dns") {
				if _, err := p2p.Discover(context.Background(), bzzPeer.Underlay, func(addr ma.Multiaddr) (stop bool, err error) {
					addresses = append(addresses, addr)
					s.logger.Infof("resolved %s in to %s", underlay, addr.String())
					return true, nil
				}); err != nil {
					tx.Rollback()
					s.logger.Errorf("addNeighboursAsPeers: invalid protocol: %v", err)
					continue
				}
			} else {
				addresses = append(addresses, bzzPeer.Underlay)
			}

			for _, addr := range addresses {
				cols1 := strings.Split(addr.String(), "/")
				if len(cols1) != 7 {
					tx.Rollback()
					s.logger.Errorf("addNeighboursAsPeers: invalid underlay format %s", underlay)
					continue
				}
				_, err = statement.Exec(overlay, cols1[1], cols1[2], cols1[3], cols1[4], cols1[6], 0, 0, 0)
				if err != nil {
					if strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry") {
						s.logger.Debugf("addNeighboursAsPeers: peer %s already got added in PEER_INFO, so ignoring it", overlay)
						continue
					}
					tx.Rollback()
					s.logger.Errorf("addNeighboursAsPeers: could not execute statement: %v", err)
					continue
				}
			}

			err = tx.Commit()
			if err != nil {
				tx.Rollback()
				s.logger.Errorf("addNeighboursAsPeers: error in commit: %v", err)
				continue
			}

			s.logger.Debugf("addNeighboursAsPeers: adding neighbour %s as new harvested peer since it is not present in PEER_INFO", overlay)
		}
	}
	return nil
}
