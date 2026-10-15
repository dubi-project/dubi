package raftrpc

import "github.com/dubi-project/dubi/internal/platform/rpc/raftwire"

// Refresh authorization on the existing TLS session, including after a Recv
// that waited across expiry and before the final acknowledgement. Already
// durable installation is never rolled back when its acknowledgement fails.
// Receive-idle deadlines belong to boundedSnapshotServer; this wrapper does
// not implement certificate revocation or force a blocked disk operation out.
type authorizedSnapshotServer struct {
	raftwire.RaftService_InstallSnapshotServer
	authorize func() error
}

func (s *authorizedSnapshotServer) Recv() (*raftwire.InstallSnapshotRequest, error) {
	if err := s.authorize(); err != nil {
		return nil, err
	}
	frame, receiveErr := s.RaftService_InstallSnapshotServer.Recv()
	if err := s.authorize(); err != nil {
		return nil, err
	}
	return frame, receiveErr
}

func (s *authorizedSnapshotServer) SendAndClose(response *raftwire.InstallSnapshotResponse) error {
	if err := s.authorize(); err != nil {
		return err
	}
	return s.RaftService_InstallSnapshotServer.SendAndClose(response)
}
