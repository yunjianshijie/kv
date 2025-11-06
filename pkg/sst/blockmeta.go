package sst

import (
	"encoding/binary"
	"hash/crc32"
	"kv/pkg/utils"
)

// BlockMeta represents metadata for a block in an SST file   表示 SST 文件中某个块的元数据
type BlockMeta struct {
	Offset   uint32 // Block offset in the SST file 偏移量
	FirstKey string // First key in the block 第一个key
	LastKey  string // Last key in the block 最后一个key
}

// NewBlockMeta creates a new BlockMeta
func NewBlockMeta(offset uint32, firstKey, lastKey string) BlockMeta {
	return BlockMeta{
		Offset:   offset,
		FirstKey: firstKey,
		LastKey:  lastKey,
	}
}

// EncodeBlockMetas 将 BlockMeta 片段编码为字节
// 每个元条目的格式：
// | offset(4) | first_key_len(2) | first_key | last_key_len(2) | last_key |
// 整体格式：
// | num_entries(4) | meta_entry1 | ... | meta_entryN | checksum(4) |
func EncodeBlockMetas(metas []BlockMeta) []byte {
	if len(metas) == 0 {
		//返回空元数据的最小编码
		result := make([]byte, 8)
		binary.LittleEndian.PutUint32(result[0:4], 0)
		checksun := crc32.ChecksumIEEE(result[0:4])
		binary.LittleEndian.PutUint32(result[4:8], checksun)
		return result
	}
	// 计算所需的总大小
	totalSize := 4 // num_entries
	for _, meta := range metas {
		totalSize += 4
		totalSize += 2 + len(meta.FirstKey)
		totalSize += 2 + len(meta.LastKey)
	}
	totalSize += 4 // checksum
	result := make([]byte, totalSize)
	pos := 0

	//写入条目数
	binary.LittleEndian.PutUint32(result[pos:pos+4], uint32(len(metas)))
	pos += 4

	// write
	for _, meta := range metas {
		// 写偏移量
		binary.LittleEndian.PutUint32(result[pos:pos+4], meta.Offset)
		pos += 4

		//写入第一个密钥长度和第一个密钥
		firstKeyLen := uint16(len(metas[0].FirstKey))
		binary.LittleEndian.PutUint16(result[pos:pos+2], firstKeyLen)
		pos += 2
		copy(result[pos:pos+len(meta.FirstKey)], meta.FirstKey)
		pos += len(meta.FirstKey)

		//写入最后一个密钥长度和最后一个密钥
		LastKeyLen := uint16(len(metas[0].LastKey))
		binary.LittleEndian.PutUint16(result[pos:pos+2], LastKeyLen)
		pos += 2
		copy(result[pos:pos+len(meta.LastKey)], meta.LastKey)
		pos += len(meta.LastKey)
	}
	//
	checksum := crc32.ChecksumIEEE(result[0:pos])
	binary.LittleEndian.PutUint32(result[pos:pos+4], checksum)

	return result
}

// DecodeBlockMetas decodes BlockMeta slice from bytes 从字节解码 BlockMeta 切片
func DecodeBlockMetas(data []byte) ([]BlockMeta, error) {
	if len(data) < 8 {
		return nil, utils.ErrInvalidMetadata
	}

	pos := 0

	// Read number of entries
	numEntries := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	if numEntries == 0 {
		expectedChecksum := binary.LittleEndian.Uint32(data[4:8])
		actualChecksum := crc32.ChecksumIEEE(data[0:4])
		if expectedChecksum != actualChecksum {
			return nil, utils.ErrInvalidChecksum
		}
		return []BlockMeta{}, nil
	}

	metas := make([]BlockMeta, numEntries)

	// Read each meta entry
	for i := 0; i < int(numEntries); i++ {
		if pos+4 > len(data) {
			return nil, utils.ErrInvalidMetadata
		}

		// Read offset
		offset := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		// Read first key
		if pos+2 > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		firstKeyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(firstKeyLen) > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		firstKey := string(data[pos : pos+int(firstKeyLen)])
		pos += int(firstKeyLen)

		// Read last key
		if pos+2 > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		lastKeyLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		if pos+int(lastKeyLen) > len(data) {
			return nil, utils.ErrInvalidMetadata
		}
		lastKey := string(data[pos : pos+int(lastKeyLen)])
		pos += int(lastKeyLen)

		metas[i] = BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		}

	}
	// Verify checksum
	if pos+4 > len(data) {
		return nil, utils.ErrInvalidMetadata
	}
	expectedChecksum := binary.LittleEndian.Uint32(data[pos : pos+4])
	actualChecksum := crc32.ChecksumIEEE(data[0:pos])
	if expectedChecksum != actualChecksum {
		return nil, utils.ErrInvalidChecksum
	}

	return metas, nil

}

// Size returns the encoded size of this BlockMeta
func (bm *BlockMeta) Size() int {
	return 4 + 2 + len(bm.FirstKey) + 2 + len(bm.LastKey)
}

// Contains checks if a key falls within this block's range
func (bm *BlockMeta) Contains(key string) bool {
	return key >= bm.FirstKey && key <= bm.LastKey
}
