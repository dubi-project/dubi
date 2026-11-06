package enginestate

// PublicationConfirmed reports a historical durability event for the exact
// current reference. It does not authorize reads or assert current holdings.
// Engines may retain this fact when superseding a value to replay an earlier
// successful operation without mistaking its old value for current data.
func (v ReadView) PublicationConfirmed(key []byte) (bool, error) {
	geo, found, err := v.GeoCoverage(key)
	if err != nil {
		return false, err
	}
	if found && geo.Index != 0 {
		return true, nil
	}
	local, found, err := v.LocalConfirmation(key)
	if err != nil {
		return false, err
	}
	// Small Regions require Geo coverage; a local receipt alone is insufficient.
	return found && len(local.Claim.Nodes) >= 3, nil
}

func (tx *Transaction) PublicationConfirmed(key []byte) (bool, error) {
	return (ReadView{state: tx.state, keyspace: tx.keyspace}).PublicationConfirmed(key)
}
