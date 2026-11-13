package codexsdk

import (
	"crypto/sha1"
	"crypto/sha256"
	"unicode/utf8"
)

// appendJSONString 按 serde_json 默认规则把 s 写成 JSON 字符串：`"`、`\`、
// \b \f \n \r \t 用短转义，其余 < 0x20 的控制字符写 \u00xx（小写十六进制）；
// 不做 HTML 转义（< > & 原样）；非 ASCII 原样（非法 UTF-8 字节写成 U+FFFD）。
// 官方 client_metadata 与请求体都经 serde_json 序列化，所以这里不能用
// encoding/json（它会转义 < > & 与 U+2028/U+2029）。
func appendJSONString(dst []byte, s string) []byte {
	return appendJSONStringMode(dst, s, false)
}

// appendASCIIJSONString 等价官方 to_ascii_json_string 的字符串规则：在 serde_json
// 规则之上，非 ASCII 字符写成小写十六进制 \uxxxx，BMP 外字符写成 UTF-16 代理对。
// turn-metadata 同时出现在 HTTP 头里，必须是纯 ASCII。
func appendASCIIJSONString(dst []byte, s string) []byte {
	return appendJSONStringMode(dst, s, true)
}

func appendJSONStringMode(dst []byte, s string, ascii bool) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch c {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			if ascii {
				dst = append(dst, `\ufffd`...)
			} else {
				dst = append(dst, "\uFFFD"...)
			}
			i++
			start = i
			continue
		}
		if !ascii {
			i += size
			continue
		}
		dst = append(dst, s[start:i]...)
		writeUnit := func(u uint16) {
			dst = append(dst, '\\', 'u', hex[u>>12], hex[(u>>8)&0xf], hex[(u>>4)&0xf], hex[u&0xf])
		}
		if r >= 0x10000 {
			v := r - 0x10000
			writeUnit(uint16(0xD800 + (v >> 10)))
			writeUnit(uint16(0xDC00 + (v & 0x3ff)))
		} else {
			writeUnit(uint16(r))
		}
		i += size
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// uuidNamespaceOID 是 RFC 9562 的 NAMESPACE_OID（6ba7b812-9dad-11d1-80b4-00c04fd430c8）。
var uuidNamespaceOID = [16]byte{0x6b, 0xa7, 0xb8, 0x12, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

// uuidV5 按 RFC 9562 计算 UUIDv5：SHA-1(namespace ‖ name) 前 16 字节，置版本 5
// 与变体 10（与 Rust uuid::Uuid::new_v5 逐字节一致）。
func uuidV5(ns [16]byte, name []byte) [16]byte {
	h := sha1.New()
	h.Write(ns[:])
	h.Write(name)
	var sum [sha1.Size]byte
	h.Sum(sum[:0])
	var u [16]byte
	copy(u[:], sum[:16])
	u[6] = u[6]&0x0f | 0x50
	u[8] = u[8]&0x3f | 0x80
	return u
}

// formatUUID 把 16 字节写成 8-4-4-4-12 小写形态。
func formatUUID(b [16]byte) string {
	var s [36]byte
	hexEncode(s[0:8], b[0:4])
	s[8] = '-'
	hexEncode(s[9:13], b[4:6])
	s[13] = '-'
	hexEncode(s[14:18], b[6:8])
	s[18] = '-'
	hexEncode(s[19:23], b[8:10])
	s[23] = '-'
	hexEncode(s[24:36], b[10:16])
	return string(s[:])
}

// instructionsIDV5 是非 lite 指令消息的 id 主体：官方
// UUIDv5(UUIDv5(NAMESPACE_OID, thread_id), 指令原文)（core/src/client.rs）。
func instructionsIDV5(threadID string, text []byte) string {
	prefix := uuidV5(uuidNamespaceOID, []byte(threadID))
	return formatUUID(uuidV5(prefix, text))
}

// liteItemID 是 lite 插入项（工具项 / 指令消息）的内容派生 id 主体：UUIDv7，
// 时间戳取 tsSource（窗口 UUID，空时线程号）的 48 位毫秒时间戳，随机位取
// SHA-256(thread ‖ kind ‖ content) 的前 10 字节，版本/变体按 RFC 9562。
// 同一线程、同一窗口、同一内容 → 同一 id（官方是窗口开始时生成并写进历史）。
func liteItemID(tsSource, threadID, kind string, content []byte) string {
	var b [16]byte
	ts, ok := uuidTimestamp(tsSource)
	if !ok {
		// 非 UUID 形态的线程号：取稳定的哈希字节充当时间戳，保证同内容同 id。
		sum := sha256.Sum256([]byte(tsSource))
		copy(b[0:6], sum[:6])
	} else {
		b[0] = byte(ts >> 40)
		b[1] = byte(ts >> 32)
		b[2] = byte(ts >> 24)
		b[3] = byte(ts >> 16)
		b[4] = byte(ts >> 8)
		b[5] = byte(ts)
	}
	h := sha256.New()
	h.Write([]byte(threadID))
	h.Write([]byte(kind))
	h.Write(content)
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	copy(b[6:], sum[:10])
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	return formatUUID(b)
}

// uuidTimestamp 取 UUID 字符串前 48 位（前 12 个十六进制位，跳过连字符）。
func uuidTimestamp(s string) (uint64, bool) {
	var v uint64
	n := 0
	for i := 0; i < len(s) && n < 12; i++ {
		c := s[i]
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		case c == '-':
			continue
		default:
			return 0, false
		}
		v = v<<4 | uint64(d)
		n++
	}
	return v, n == 12
}
