package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"golang.org/x/crypto/argon2"
	"unicode/utf8"
)

// Bound the memory-hard work per process, including concurrent HTTP gateways.
var passwordSlot = make(chan struct{}, 1)

func ValidatePassword(v string) error {
	if !utf8.ValidString(v) || utf8.RuneCountInString(v) < 12 || len(v) > 256 {
		return errors.New("密码需要至少 12 个字符，且不超过 256 字节。")
	}
	return nil
}
func passwordKey(ctx context.Context, password string, salt []byte) ([]byte, error) {
	select {
	case passwordSlot <- struct{}{}:
		defer func() { <-passwordSlot }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	key := argon2.IDKey([]byte(password), salt, 3, 64*1024, 1, 32)
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	return key, nil
}
func SetPassword(ctx context.Context, a *rootstate.AdminAccount, password string) error {
	if e := ValidatePassword(password); e != nil {
		return e
	}
	salt := make([]byte, 16)
	if _, e := rand.Read(salt); e != nil {
		return e
	}
	key, e := passwordKey(ctx, password, salt)
	if e != nil {
		return e
	}
	a.PasswordSalt = hex.EncodeToString(salt)
	a.PasswordHash = hex.EncodeToString(key)
	a.PasswordScheme = "argon2id-v19-m65536-t3-p1"
	return nil
}
func CheckPassword(ctx context.Context, a rootstate.AdminAccount, password string) (bool, error) {
	if len(password) > 256 {
		return false, nil
	}
	salt, _ := hex.DecodeString(a.PasswordSalt)
	want, _ := hex.DecodeString(a.PasswordHash)
	if len(salt) != 16 {
		salt = make([]byte, 16)
	}
	key, e := passwordKey(ctx, password, salt)
	if e != nil {
		return false, e
	}
	return subtle.ConstantTimeCompare(key, want) == 1, nil
}
func GenerateAccountSession(a rootstate.AdminAccount) (string, rootstate.AccessCredential, error) {
	return generateAccountSession(a)
}
