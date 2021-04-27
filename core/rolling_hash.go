package core

import (
	"hash"
	"hash/adler32"
)

const (
	Mod = 65521
)

// Using Adler32 - same as rsync
type Radler32 struct {
	a, b uint32
	n    uint32

	// windows is a circular buffer
	window []byte
	oldest int

	adler32 hash.Hash32
}

// Reset resets the digest
func (r *Radler32) Reset() {
	r.window = r.window[:0] // Reset the size but don't reallocate
	r.oldest = 0
	r.a = 1
	r.b = 0
	r.n = 0
	r.adler32.Reset()
}

// Pour returns a new Adler32 digest
func Pour(size int64) *Radler32 {
	return &Radler32{
		a:       1,
		b:       0,
		n:       0,
		window:  make([]byte, 0, size),
		oldest:  0,
		adler32: adler32.New(),
	}
}

// Write **appends** data to the rolling window and updates the digest.
func (r *Radler32) Write(data []byte) (int, error) {
	l := len(data)
	if l == 0 {
		return 0, nil
	}

	// shuffle
	n := len(r.window)
	if r.oldest != 0 {
		tmp := make([]byte, r.oldest)
		copy(tmp, r.window[:r.oldest])
		copy(r.window, r.window[r.oldest:])
		copy(r.window[n-r.oldest:], tmp)
		r.oldest = 0
	}
	r.window = append(r.window, data...)

	r.adler32.Reset()
	r.adler32.Write(r.window)
	s := r.adler32.Sum32()
	r.a, r.b = s&0xffff, s>>16
	r.n = uint32(len(r.window)) % Mod
	return len(data), nil
}

// Sum32 returns the hash as a uint32
func (r *Radler32) Sum32() uint32 {
	return r.b<<16 | r.a
}

// enter new byte, return the oldest byte
func (r *Radler32) Roll(b byte) byte {

	// extract the entering/leaving bytes and update the circular buffer.
	enter := uint32(b)
	leave := uint32(r.window[r.oldest])
	r.window[r.oldest] = b
	r.oldest += 1
	if r.oldest >= len(r.window) {
		r.oldest = 0
	}

	r.a = (r.a + Mod + enter - leave) % Mod
	r.b = (r.b + (r.n*leave/Mod+1)*Mod + r.a - (r.n * leave) - 1) % Mod
	return byte(leave)
}
