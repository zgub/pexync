package core

import (
	"hash"
	"hash/adler32"
	"io"

	"github.com/spf13/viper"
)

const (
	Mod  = 65521
	Size = 4
)

// Adler32 is a digest which satisfies the rollinghash.Hash32 interface.
// It implements the adler32 algorithm https://en.wikipedia.org/wiki/Adler-32
type Radler32 struct {
	a, b uint32
	n    uint32

	// window is treated like a circular buffer, where the oldest element
	// is indicated by d.oldest
	window []byte
	oldest int

	adler32 hash.Hash32
}

// Reset resets the digest to its initial state.
func (d *Radler32) Reset() {
	d.window = d.window[:0] // Reset the size but don't reallocate
	d.oldest = 0
	d.a = 1
	d.b = 0
	d.n = 0
	d.adler32.Reset()
}

// Pour returns a new Adler32 digest
func Pour() *Radler32 {
	return &Radler32{
		a:       1,
		b:       0,
		n:       0,
		window:  make([]byte, 0, viper.GetInt("block_size")),
		oldest:  0,
		adler32: adler32.New(),
	}
}

// Size is 4 bytes
func (d *Radler32) Size() int { return Size }

// BlockSize is 1 byte
func (d *Radler32) BlockSize() int { return 1 }

// WriteWindow writes the contents of the current window to w.
func (d *Radler32) WriteWindow(w io.Writer) (n int, err error) {
	// Copy the older bytes.
	if d.oldest < len(d.window) {
		n, err = w.Write(d.window[d.oldest:])
	}
	// Then the newer bytes.
	if err == nil && d.oldest > 0 {
		var n2 int
		n2, err = w.Write(d.window[:d.oldest])
		n += n2
	}
	return
}

// Write appends data to the rolling window and updates the digest.
func (d *Radler32) Write(data []byte) (int, error) {
	l := len(data)
	if l == 0 {
		return 0, nil
	}
	// Re-arrange the window so that the leftmost element is at index 0
	n := len(d.window)
	if d.oldest != 0 {
		tmp := make([]byte, d.oldest)
		copy(tmp, d.window[:d.oldest])
		copy(d.window, d.window[d.oldest:])
		copy(d.window[n-d.oldest:], tmp)
		d.oldest = 0
	}
	d.window = append(d.window, data...)

	// Piggy-back on the core implementation
	d.adler32.Reset()
	d.adler32.Write(d.window)
	s := d.adler32.Sum32()
	d.a, d.b = s&0xffff, s>>16
	d.n = uint32(len(d.window)) % Mod
	return len(data), nil
}

// Sum32 returns the hash as a uint32
func (d *Radler32) Sum32() uint32 {
	return d.b<<16 | d.a
}

// Sum returns the hash as a byte slice
func (d *Radler32) Sum(b []byte) []byte {
	v := d.Sum32()
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// Roll updates the checksum of the window from the entering byte. You
// MUST initialize a window with Write() before calling this method.
func (d *Radler32) Roll(b byte) {
	// This check costs 10-15% performance. If we disable it, we crash
	// when the window is empty. If we enable it, we are always correct
	// (an empty window never changes no matter how much you roll it).
	//if len(d.window) == 0 {
	//	return
	//}
	// extract the entering/leaving bytes and update the circular buffer.
	enter := uint32(b)
	leave := uint32(d.window[d.oldest])
	d.window[d.oldest] = b
	d.oldest += 1
	if d.oldest >= len(d.window) {
		d.oldest = 0
	}

	// See http://stackoverflow.com/questions/40985080/why-does-my-rolling-adler32-checksum-not-work-in-go-modulo-arithmetic
	d.a = (d.a + Mod + enter - leave) % Mod
	d.b = (d.b + (d.n*leave/Mod+1)*Mod + d.a - (d.n * leave) - 1) % Mod
}
