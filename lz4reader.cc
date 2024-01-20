#include "libpstack/lz4reader.h"

#include <endian.h>
#include <lz4.h>

#include <algorithm>

namespace
{
    template<typename T>
    T FetchAdd(T &offset, size_t incre)
    {
        auto bak = offset;
        offset += incre;
        return bak;
    }
}

namespace pstack
{
    Lz4Reader::Lz4Reader(Reader::csptr upstream)
        : upstream_{std::move(upstream)}
    {
        Off offset = 0;
        uint32_t magic;
        upstream_->readObj(FetchAdd(offset, sizeof(uint32_t)), &magic);
        if (le32toh(magic) != 0x184d2204)
        {
            return;
        }
        uint8_t flg;
        upstream_->readObj(FetchAdd(offset, sizeof(uint8_t)), &flg);
        if ((flg & 0b01100000) != 0b01100000)
        {
            // version check and make sure lz4 is in block independence mode
            return;
        }
        with_block_checksum_ = (flg & 0b00010000);
        with_content_size_ = (flg & 0b00001000);
        with_content_checksum_ = (flg & 0b00000100);
        with_dic_id_ = (flg & 0b00000001);
        uint8_t bd_byte;
        upstream_->readObj(FetchAdd(offset, sizeof(uint8_t)), &bd_byte);
        switch ((bd_byte & 0b01110000) >> 4)
        {
        case 4:
            max_block_size_ = 64 * 1024;
            break;
        case 5:
            max_block_size_ = 256 * 1024;
            break;
        case 6:
            max_block_size_ = 1 * 1024 * 1024;
            break;
        case 7:
            max_block_size_ = 4 * 1024 * 1024;
            break;
        default:
            break;
        }
        if (max_block_size_ == 0)
        {
            return;
        }
        block_buf_.resize(max_block_size_);
        decompressed_buf_.resize(max_block_size_);
        if (with_content_size_)
        {
            // should be not
            offset += 8;
        }
        if (with_dic_id_)
        {
            offset += 4;
        }
        offset += 1; // header checksum

        uint32_t block_size = 0;
        do
        {
            upstream_->readObj(FetchAdd(offset, sizeof(block_size)), &block_size);
            block_size = le32toh(block_size);
            BlockInfo blk_info {
                .uncompressed_ = (block_size >> 31) ? true : false,
                .data_size_ = block_size & 0x7fffffff,
                .data_offset_ = offset,
            };
            if (blk_info.data_size_ > max_block_size_)
            {
                return;
            }
            if (blk_info.data_size_ > 0)
            {
                blocks_.push_back(blk_info);
                offset += blk_info.data_size_;
                if (with_block_checksum_)
                {
                    offset += 4;
                }
            }
            // block_size == 0 means this is an end mark instead of a data block
        } while (block_size > 0);
        if (with_content_checksum_)
        {
            offset += 4;
        }

        if (offset != upstream_->size())
        {
            // we only support simple lz4 file with only one lz4 frame
            return;
        }

        if (!blocks_.empty())
        {
            int last_block_decompressed_size = 0;
            if (blocks_.back().uncompressed_)
            {
                last_block_decompressed_size = blocks_.back().data_size_;
            }
            else
            {
                if (!read_upstream_up_to(blocks_.back().data_offset_, blocks_.back().data_size_, block_buf_.data()))
                {
                    return;
                }
                last_block_decompressed_size = LZ4_decompress_safe_partial(
                    block_buf_.data(), decompressed_buf_.data(), blocks_.back().data_size_, max_block_size_, max_block_size_);
                if (last_block_decompressed_size < 0) {
                    return;
                }
                decompressed_buf_.resize(last_block_decompressed_size);
                decompressed_buf_blk_id_ = blocks_.size() - 1;
            }
            decompressed_size_ = (blocks_.size() - 1) * max_block_size_ + last_block_decompressed_size;
        }
    }

    Lz4Reader::~Lz4Reader() {}

    size_t Lz4Reader::read(Off decompressed_offset, size_t req_read_size, char *dst) const
    {
        // offsets: floor(block boundry) -- start -- (might cross multiple blocks) -- end -- ceil(block boundry)
        const Reader::Off offset_end = decompressed_offset + req_read_size;
        if (offset_end > decompressed_size_)
        {
            throw Exception() << "read beyond decompressed data length";
        }
        const size_t blk_idx_start = decompressed_offset / max_block_size_;
        const Reader::Off offset_floor = blk_idx_start * max_block_size_;
        for (Reader::Off offset = offset_floor; offset < offset_end; offset += max_block_size_)
        {
            const size_t blk_idx = blk_idx_start + (offset - offset_floor) / max_block_size_;
            const auto offset_start_within_blk = (decompressed_offset > offset) ? decompressed_offset - offset : 0;
            const auto offset_end_within_blk = std::min(max_block_size_, decompressed_offset + req_read_size - offset);
            const auto req_read_len_within_blk = offset_end_within_blk - offset_start_within_blk;
            if (!decompress_block(blk_idx, offset_start_within_blk, req_read_len_within_blk, FetchAdd(dst, req_read_len_within_blk)))
            {
                throw Exception() << "Failed to read/decompress blk " << blk_idx;
            }
        }
        return req_read_size;
    }

    void Lz4Reader::describe(std::ostream &os) const
    {
        os << "lz4 compressed " << *upstream_;
    }

    Reader::Off Lz4Reader::size() const { return decompressed_size_; }

    bool Lz4Reader::decompress_block(size_t blk_idx, Off decompressed_offset, size_t req_read_size, char *dst) const {
        auto& blk = blocks_[blk_idx];
        if (blk.uncompressed_)
        {
            return read_upstream_up_to(blk.data_offset_ + decompressed_offset, req_read_size, dst);
        }

        // might hit previusely cached decompressed buffer
        if (decompressed_buf_blk_id_ != blk_idx)
        {
            if (!read_upstream_up_to(blk.data_offset_, blk.data_size_, block_buf_.data())) {
                return false;
            }
            decompressed_buf_blk_id_ = std::numeric_limits<decltype(decompressed_buf_blk_id_)>::max();
            decompressed_buf_.resize(max_block_size_); // should not trigger re-alloc
            int decompressed_size = LZ4_decompress_safe_partial(
                block_buf_.data(), decompressed_buf_.data(), blk.data_size_, max_block_size_, max_block_size_);
            if (decompressed_size < 0) {
                return false;
            }
            decompressed_buf_.resize(decompressed_size);
            decompressed_buf_blk_id_ = blk_idx;
        }
        if (decompressed_buf_.size() < decompressed_offset + req_read_size) {
            return false;
        }
        std::copy_n(decompressed_buf_.data() + decompressed_offset, req_read_size, dst);
        return true;
    }

    bool Lz4Reader::read_upstream_up_to(Off offset, size_t req_size, char *dst) const
    {
        uint32_t read_sum_cnt = 0;
        while (req_size > read_sum_cnt)
        {
            uint32_t read_size = upstream_->read(
                offset + read_sum_cnt,
                req_size - read_sum_cnt,
                dst + read_sum_cnt);
            if (read_size == 0)
            {
                // should not happend: EOF
                return false;
            }
            read_sum_cnt += read_size;
        }
        return true;
    }

}