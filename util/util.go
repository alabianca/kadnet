package util

import "encoding/hex"

func BytesFromHex(in string) ([]byte, error) {
	src := []byte(in)
	out := make([]byte, hex.DecodedLen(len(src)))

	n, err := hex.Decode(out, src)
	if err != nil {
		return nil, err
	}

	return out[:n], nil

}
