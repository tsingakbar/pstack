#ifndef LIBPSTACK_LZ4READER_H
#define LIBPSTACK_LZ4READER_H
#include "libpstack/reader.h"

#include <vector>

namespace pstack {
/*
 * Provides an Lz4-decoded view of downstream.
 * official lz4 frame format won't provide random access information, so we have to sparsely scan the whole file first.
 * TODO: produce lz4 frame with random access index with the "Skippable Frames", and use them to avoid the full scan.
 */
class Lz4Reader : public Reader {
    Lz4Reader(const Lz4Reader &) = delete;
    Lz4Reader() = delete;
    Reader::csptr upstream_;
    bool with_block_checksum_ = false;
    bool with_content_size_ = false;
    bool with_content_checksum_ = false;
    bool with_dic_id_ = false;
    size_t max_block_size_ = 0;
    mutable std::vector<char> block_buf_, decompressed_buf_;
    mutable size_t decompressed_buf_blk_id_ = std::numeric_limits<size_t>::max();
    struct BlockInfo {
        bool uncompressed_;
        size_t data_size_;
        Reader::Off data_offset_;
    };
    std::vector<BlockInfo> blocks_;
    size_t decompressed_size_ = 0;
public:
    Lz4Reader(Reader::csptr upstream);
    ~Lz4Reader();
    size_t read(Off, size_t, char *) const override;
    void describe(std::ostream &) const override;
    Off size() const override;
    std::string filename() const override { return upstream_->filename(); }

private:
    bool decompress_block(size_t, Off, size_t, char*) const;
    bool read_upstream_up_to(Off, size_t, char*) const;
};
}

#endif
