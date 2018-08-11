/*
Sniperkit-Bot
- Date: 2018-08-11 22:33:29.968631097 +0200 CEST m=+0.112171202
- Status: analyzed
*/

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package mem

// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// idsMapHash is the hash for a given map entry, this is public to support
// iterating over the map using a native Go for loop.
type idsMapHash uint64

// idsMapHashFn is the hash function to execute when hashing a key.
type idsMapHashFn func([]byte) idsMapHash

// idsMapEqualsFn is the equals key function to execute when detecting equality of a key.
type idsMapEqualsFn func([]byte, []byte) bool

// idsMapCopyFn is the copy key function to execute when copying the key.
type idsMapCopyFn func([]byte) []byte

// idsMapFinalizeFn is the finalize key function to execute when finished with a key.
type idsMapFinalizeFn func([]byte)

// idsMap uses the genny package to provide a generic hash map that can be specialized
// by running the following command from this root of the repository:
// ```
// make hashmap-gen pkg=outpkg key_type=Type value_type=Type out_dir=/tmp
// ```
// Or if you would like to use bytes or ident.ID as keys you can use the
// partially specialized maps to generate your own maps as well:
// ```
// make byteshashmap-gen pkg=outpkg value_type=Type out_dir=/tmp
// make idhashmap-gen pkg=outpkg value_type=Type out_dir=/tmp
// ```
// This will output to stdout the generated source file to use for your map.
// It uses linear probing by incrementing the number of the hash created when
// hashing the identifier if there is a collision.
// idsMap is a value type and not an interface to allow for less painful
// upgrades when adding/removing methods, it is not likely to need mocking so
// an interface would not be super useful either.
type idsMap struct {
	_idsMapOptions

	// lookup uses hash of the identifier for the key and the MapEntry value
	// wraps the value type and the key (used to ensure lookup is correct
	// when dealing with collisions), we use uint64 for the hash partially
	// because lookups of maps with uint64 keys has a fast path for Go.
	lookup map[idsMapHash]idsMapEntry
}

// _idsMapOptions is a set of options used when creating an identifier map, it is kept
// private so that implementers of the generated map can specify their own options
// that partially fulfill these options.
type _idsMapOptions struct {
	// hash is the hash function to execute when hashing a key.
	hash idsMapHashFn
	// equals is the equals key function to execute when detecting equality.
	equals idsMapEqualsFn
	// copy is the copy key function to execute when copying the key.
	copy idsMapCopyFn
	// finalize is the finalize key function to execute when finished with a
	// key, this is optional to specify.
	finalize idsMapFinalizeFn
	// initialSize is the initial size for the map, use zero to use Go's std map
	// initial size and consequently is optional to specify.
	initialSize int
}

// idsMapEntry is an entry in the map, this is public to support iterating
// over the map using a native Go for loop.
type idsMapEntry struct {
	// key is used to check equality on lookups to resolve collisions
	key _idsMapKey
	// value type stored
	value struct{}
}

type _idsMapKey struct {
	key      []byte
	finalize bool
}

// Key returns the map entry key.
func (e idsMapEntry) Key() []byte {
	return e.key.key
}

// Value returns the map entry value.
func (e idsMapEntry) Value() struct{} {
	return e.value
}

// _idsMapAlloc is a non-exported function so that when generating the source code
// for the map you can supply a public constructor that sets the correct
// hash, equals, copy, finalize options without users of the map needing to
// implement them themselves.
func _idsMapAlloc(opts _idsMapOptions) *idsMap {
	m := &idsMap{_idsMapOptions: opts}
	m.Reallocate()
	return m
}

func (m *idsMap) newMapKey(k []byte, opts _idsMapKeyOptions) _idsMapKey {
	key := _idsMapKey{key: k, finalize: opts.finalizeKey}
	if !opts.copyKey {
		return key
	}

	key.key = m.copy(k)
	return key
}

func (m *idsMap) removeMapKey(hash idsMapHash, key _idsMapKey) {
	delete(m.lookup, hash)
	if key.finalize {
		m.finalize(key.key)
	}
}

// Get returns a value in the map for an identifier if found.
func (m *idsMap) Get(k []byte) (struct{}, bool) {
	hash := m.hash(k)
	for entry, ok := m.lookup[hash]; ok; entry, ok = m.lookup[hash] {
		if m.equals(entry.key.key, k) {
			return entry.value, true
		}
		// Linear probe to "next" to this entry (really a rehash)
		hash++
	}
	var empty struct{}
	return empty, false
}

// Set will set the value for an identifier.
func (m *idsMap) Set(k []byte, v struct{}) {
	m.set(k, v, _idsMapKeyOptions{
		copyKey:     true,
		finalizeKey: m.finalize != nil,
	})
}

// idsMapSetUnsafeOptions is a set of options to use when setting a value with
// the SetUnsafe method.
type idsMapSetUnsafeOptions struct {
	NoCopyKey     bool
	NoFinalizeKey bool
}

// SetUnsafe will set the value for an identifier with unsafe options for how
// the map treats the key.
func (m *idsMap) SetUnsafe(k []byte, v struct{}, opts idsMapSetUnsafeOptions) {
	m.set(k, v, _idsMapKeyOptions{
		copyKey:     !opts.NoCopyKey,
		finalizeKey: !opts.NoFinalizeKey,
	})
}

type _idsMapKeyOptions struct {
	copyKey     bool
	finalizeKey bool
}

func (m *idsMap) set(k []byte, v struct{}, opts _idsMapKeyOptions) {
	hash := m.hash(k)
	for entry, ok := m.lookup[hash]; ok; entry, ok = m.lookup[hash] {
		if m.equals(entry.key.key, k) {
			m.lookup[hash] = idsMapEntry{
				key:   entry.key,
				value: v,
			}
			return
		}
		// Linear probe to "next" to this entry (really a rehash)
		hash++
	}

	m.lookup[hash] = idsMapEntry{
		key:   m.newMapKey(k, opts),
		value: v,
	}
}

// Iter provides the underlying map to allow for using a native Go for loop
// to iterate the map, however callers should only ever read and not write
// the map.
func (m *idsMap) Iter() map[idsMapHash]idsMapEntry {
	return m.lookup
}

// Len returns the number of map entries in the map.
func (m *idsMap) Len() int {
	return len(m.lookup)
}

// Contains returns true if value exists for key, false otherwise, it is
// shorthand for a call to Get that doesn't return the value.
func (m *idsMap) Contains(k []byte) bool {
	_, ok := m.Get(k)
	return ok
}

// Delete will remove a value set in the map for the specified key.
func (m *idsMap) Delete(k []byte) {
	hash := m.hash(k)
	for entry, ok := m.lookup[hash]; ok; entry, ok = m.lookup[hash] {
		if m.equals(entry.key.key, k) {
			m.removeMapKey(hash, entry.key)
			return
		}
		// Linear probe to "next" to this entry (really a rehash)
		hash++
	}
}

// Reset will reset the map by simply deleting all keys to avoid
// allocating a new map.
func (m *idsMap) Reset() {
	for hash, entry := range m.lookup {
		m.removeMapKey(hash, entry.key)
	}
}

// Reallocate will avoid deleting all keys and reallocate a new
// map, this is useful if you believe you have a large map and
// will not need to grow back to a similar size.
func (m *idsMap) Reallocate() {
	if m.initialSize > 0 {
		m.lookup = make(map[idsMapHash]idsMapEntry, m.initialSize)
	} else {
		m.lookup = make(map[idsMapHash]idsMapEntry)
	}
}
