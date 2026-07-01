package wallet

import (
	"fmt"
)

// ResolveHotkeySS58 returns the orchestrator hotkey SS58 address.
// Priority: ORCHESTRATOR_HOTKEY env, then bittensor wallet hotkey file.
func ResolveHotkeySS58(walletPath, walletName, hotkeyName, override string) (string, error) {
	if override != "" {
		return override, nil
	}
	signer, err := LoadSigner(walletPath, walletName, hotkeyName)
	if err != nil {
		return "", err
	}
	return signer.SS58Address(), nil
}

// ResolveSigner loads the orchestrator hotkey signer unless ORCHESTRATOR_HOTKEY override is set without wallet.
func ResolveSigner(walletPath, walletName, hotkeyName, override string) (*Signer, error) {
	if override != "" {
		return nil, fmt.Errorf("ORCHESTRATOR_HOTKEY is set without wallet file; cannot sign auth challenges")
	}
	return LoadSigner(walletPath, walletName, hotkeyName)
}
