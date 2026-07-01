package wallet

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	sr25519 "github.com/ChainSafe/go-schnorrkel"
)

// Signer signs messages with the orchestrator hotkey (sr25519, same as bittensor).
type Signer struct {
	hotkeySS58 string
	pair       *sr25519.Keypair
}

type hotkeyDoc struct {
	PrivateKey   string `json:"privateKey"`
	SecretSeed   string `json:"secretSeed"`
	SecretPhrase string `json:"secretPhrase"`
	SS58Address  string `json:"ss58Address"`
}

// LoadSigner loads an unencrypted bittensor hotkey for signing.
func LoadSigner(walletPath, walletName, hotkeyName string) (*Signer, error) {
	path := resolveWalletPath(walletPath)
	hotkeyPath := filepath.Join(path, walletName, "hotkeys", hotkeyName)
	raw, err := os.ReadFile(hotkeyPath)
	if err != nil {
		return nil, fmt.Errorf("read hotkey %s: %w", hotkeyPath, err)
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("hotkey %s appears encrypted; set BEAMCORE_API_KEY or use an unencrypted hotkey", hotkeyPath)
	}
	var doc hotkeyDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse hotkey %s: %w", hotkeyPath, err)
	}
	if doc.SS58Address == "" {
		return nil, fmt.Errorf("hotkey %s missing ss58Address", hotkeyPath)
	}

	pair, err := keypairFromDoc(doc)
	if err != nil {
		return nil, err
	}
	return &Signer{hotkeySS58: doc.SS58Address, pair: pair}, nil
}

func keypairFromDoc(doc hotkeyDoc) (*sr25519.Keypair, error) {
	if doc.PrivateKey != "" {
		b, err := decodeHexKey(doc.PrivateKey)
		if err != nil {
			return nil, err
		}
		if len(b) == 64 {
			var ed [64]byte
			copy(ed[:], b)
			sk := sr25519.NewSecretKeyFromEd25519Bytes(ed)
			pub, err := sk.Public()
			if err != nil {
				return nil, err
			}
			return sr25519.NewKeypair(pub, sk), nil
		}
		if len(b) == 32 {
			var seed [32]byte
			copy(seed[:], b)
			return keypairFromSeed(seed)
		}
		return nil, fmt.Errorf("privateKey must be 32 or 64 bytes, got %d", len(b))
	}
	if doc.SecretSeed != "" {
		b, err := decodeHexKey(doc.SecretSeed)
		if err != nil {
			return nil, err
		}
		if len(b) != 32 {
			return nil, fmt.Errorf("secretSeed must be 32 bytes, got %d", len(b))
		}
		var seed [32]byte
		copy(seed[:], b)
		return keypairFromSeed(seed)
	}
	if doc.SecretPhrase != "" {
		return nil, fmt.Errorf("mnemonic hotkeys are not supported in Go yet; use unencrypted hotkey file or BEAMCORE_API_KEY")
	}
	return nil, fmt.Errorf("hotkey file has no secretSeed or privateKey")
}

func keypairFromSeed(seed [32]byte) (*sr25519.Keypair, error) {
	msk, err := sr25519.NewMiniSecretKeyFromRaw(seed)
	if err != nil {
		return nil, err
	}
	sk := msk.ExpandEd25519()
	pub := msk.Public()
	if pub == nil {
		return nil, fmt.Errorf("failed to derive public key from seed")
	}
	return sr25519.NewKeypair(pub, sk), nil
}

func decodeHexKey(s string) ([]byte, error) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "0x")
	return hex.DecodeString(s)
}

// SS58Address returns the hotkey public address.
func (s *Signer) SS58Address() string {
	return s.hotkeySS58
}

// SignHex returns a 0x-prefixed hex signature for message (matches Python wallet.hotkey.sign).
func (s *Signer) SignHex(message string) (string, error) {
	t := sr25519.NewSigningContext([]byte("substrate"), []byte(message))
	sig, err := s.pair.Sign(t)
	if err != nil {
		return "", err
	}
	enc := sig.Encode()
	return "0x" + hex.EncodeToString(enc[:]), nil
}

func resolveWalletPath(walletPath string) string {
	if walletPath == "" {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, ".bittensor", "wallets")
	}
	if strings.HasPrefix(walletPath, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, walletPath[2:])
	}
	if walletPath == "~" {
		home, _ := os.UserHomeDir()
		return home
	}
	return walletPath
}
