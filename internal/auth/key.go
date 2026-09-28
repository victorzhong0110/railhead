// Package auth 只负责密钥的哈希和前缀。
// 数据库里不存明文。日志里只打前缀，避免密钥进日志系统。
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// Hash 是密钥的查找键。sha256 足够：密钥本身是高熵随机串，不需要慢哈希。
func Hash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// Prefix 留给日志和后台展示。sk- 加 8 位十六进制，对不上完整密钥。
func Prefix(raw string) string {
	if len(raw) <= 11 {
		return raw
	}
	return raw[:11]
}

// NewRawKey 生成 sk- 加 32 位十六进制。明文只在创建接口返回一次。
func NewRawKey() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "sk-" + hex.EncodeToString(buf), nil
}
