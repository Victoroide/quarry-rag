package extractor

import (
	"encoding/binary"
	"testing"
	"unicode/utf16"
)

type cfbStream struct {
	name string
	data []byte
}

type cfbOptions struct {
	declaredSize *uint32
	cyclic       bool
	embedded     []cfbStream
}

func buildCFB(t testing.TB, streams []cfbStream, opts cfbOptions) []byte {
	t.Helper()
	const (
		sectorSize = 512
		free       = uint32(0xFFFFFFFF)
		end        = uint32(0xFFFFFFFE)
		fat        = uint32(0xFFFFFFFD)
	)
	if len(streams) == 0 {
		t.Fatal("a directory requires a root stream")
	}
	rootCount := len(streams)
	entries := rootCount + 1
	if len(opts.embedded) != 0 {
		streams = append(append([]cfbStream{}, streams...), opts.embedded...)
		entries = len(streams) + 2 // Root and embedded storage.
	}
	directorySectors := (entries + 3) / 4
	sectors := make([]int, len(streams))
	dataSectors := 0
	for i, stream := range streams {
		sectors[i] = max(8, (len(stream.data)+sectorSize-1)/sectorSize)
		dataSectors += sectors[i]
	}
	fatSectors := 1
	for fatSectors*128 < directorySectors+dataSectors+fatSectors {
		fatSectors++
	}
	if fatSectors > 109 {
		t.Fatal("streams exceed the header FAT capacity")
	}
	data := make([]byte, (1+directorySectors+fatSectors+dataSectors)*sectorSize)
	binary.LittleEndian.PutUint64(data, 0xE11AB1A1E011CFD0)
	binary.LittleEndian.PutUint16(data[24:], 0x003E)
	binary.LittleEndian.PutUint16(data[26:], 3)
	binary.LittleEndian.PutUint16(data[28:], 0xFFFE)
	binary.LittleEndian.PutUint16(data[30:], 9)
	binary.LittleEndian.PutUint16(data[32:], 6)
	binary.LittleEndian.PutUint32(data[44:], uint32(fatSectors))
	binary.LittleEndian.PutUint32(data[48:], uint32(fatSectors))
	binary.LittleEndian.PutUint32(data[56:], 4096)
	binary.LittleEndian.PutUint32(data[60:], end)
	binary.LittleEndian.PutUint32(data[68:], end)
	for i := range 109 {
		value := free
		if i < fatSectors {
			value = uint32(i)
		}
		binary.LittleEndian.PutUint32(data[76+4*i:], value)
	}
	fatData := data[sectorSize : (1+fatSectors)*sectorSize]
	for i := 0; i < len(fatData); i += 4 {
		binary.LittleEndian.PutUint32(fatData[i:], free)
	}
	setFAT := func(sector int, next uint32) {
		binary.LittleEndian.PutUint32(fatData[4*sector:], next)
	}
	for i := range fatSectors {
		setFAT(i, fat)
	}
	for i := range directorySectors {
		next := uint32(fatSectors + i + 1)
		if i+1 == directorySectors {
			next = end
		}
		setFAT(fatSectors+i, next)
	}
	directory := data[(1+fatSectors)*sectorSize : (1+fatSectors+directorySectors)*sectorSize]
	writeEntry := func(index int, name string, kind byte, start, size uint32) {
		entry := directory[index*128 : (index+1)*128]
		encoded := utf16.Encode([]rune(name))
		nameUnits := len(encoded)
		if nameUnits > 31 {
			t.Fatal("stream name exceeds the directory entry")
			return
		}
		for i, character := range encoded {
			binary.LittleEndian.PutUint16(entry[2*i:], character)
		}
		nameLength := uint16(nameUnits)
		binary.LittleEndian.PutUint16(entry[64:], 2*(nameLength+1))
		entry[66], entry[67] = kind, 1
		for _, offset := range []int{68, 72, 76} {
			binary.LittleEndian.PutUint32(entry[offset:], free)
		}
		binary.LittleEndian.PutUint32(entry[116:], start)
		binary.LittleEndian.PutUint32(entry[120:], size)
	}
	writeEntry(0, "Root Entry", 5, end, 0)
	binary.LittleEndian.PutUint32(directory[76:], 1)
	start := fatSectors + directorySectors
	for i, stream := range streams {
		entry := i + 1
		if i >= rootCount {
			entry++ // Skip the embedded storage.
		}
		streamSize := len(stream.data)
		if streamSize > 7<<20 {
			t.Fatal("stream exceeds the header FAT capacity")
			return nil
		}
		size := max(uint32(4096), uint32(streamSize))
		if opts.declaredSize != nil {
			size = *opts.declaredSize
		}
		writeEntry(entry, stream.name, 2, docFixtureSize(t, start), size)
		if i+1 < len(streams) && i+1 != rootCount {
			binary.LittleEndian.PutUint32(directory[entry*128+72:], uint32(entry+1))
		}
		copy(data[(start+1)*sectorSize:], stream.data)
		for j := range sectors[i] {
			next := docFixtureSize(t, start+j+1)
			if j+1 == sectors[i] {
				next = end
			}
			if opts.cyclic {
				next = docFixtureSize(t, start+j)
			}
			setFAT(start+j, next)
		}
		start += sectors[i]
	}
	if len(opts.embedded) != 0 {
		storage := rootCount + 1
		writeEntry(storage, "Embedded document", 1, end, 0)
		binary.LittleEndian.PutUint32(directory[storage*128+76:], docFixtureSize(t, storage+1))
		binary.LittleEndian.PutUint32(directory[128+72:], docFixtureSize(t, storage))
		if rootCount > 1 {
			binary.LittleEndian.PutUint32(directory[storage*128+72:], 2)
		}
	}
	return data
}
