package raft

import (
	"errors"
	"fmt"
)

const SubmitBatchEntries = 32
const SubmitBatchBytes = 256 << 10

// SubmitBatch amortizes Ready persistence across independent proposals. A nil
// row error means proposal admission only, never quorum commit or application.
// Validation/admission errors remain per row. A shared runtime/storage failure
// returns an outer error: callers must not infer which proposals committed.
func (g *Group) SubmitBatch(commands [][]byte) ([]error, error) {
	if len(commands) == 0 || len(commands) > SubmitBatchEntries {
		return nil, errors.New("invalid Raft proposal batch count")
	}
	total := 0
	for _, command := range commands {
		if len(command) > SubmitBatchBytes-total {
			return nil, errors.New("Raft proposal batch exceeds byte bound")
		}
		total += len(command)
	}
	results := make([]error, len(commands))
	for i, command := range commands {
		if len(command) == 0 {
			results[i] = errors.New("empty command is reserved for Raft no-op entries")
		} else if validator, ok := g.state.(CommandValidator); ok {
			if err := validator.ValidateCommand(command); err != nil {
				results[i] = fmt.Errorf("validate Raft command: %w", err)
			}
		}
	}
	g.mu.Lock()
	if err := g.checkLocked(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	for i, command := range commands {
		if results[i] != nil {
			continue
		}
		if err := g.admitProposal(); err != nil {
			results[i] = err
			continue
		}
		results[i] = g.raw.Propose(command)
	}
	messages, err := g.processLocked(false)
	g.mu.Unlock()
	if err != nil {
		return nil, err
	}
	g.dispatch(messages)
	return results, nil
}

func (h *Host) SubmitBatch(groupID string, commands [][]byte) ([]error, error) {
	group, err := h.group(groupID)
	if err != nil {
		return nil, err
	}
	return group.SubmitBatch(commands)
}
