#ifndef pstack_reader_h
#define pstack_reader_h

#include <stdint.h>
#include <climits>
#include <stdlib.h>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <limits>
#include <list>
#include <cassert>
#include "libpstack/exception.h"

namespace pstack {

// Reader provides the basic random-access IO to a range of bytes.  The most
// basic reader is a FileReader, which allows you to access the content of a
// file from offset 0 through to the length of the file.
//
// You can compose readers on top of this - a FileReader can be wrapped in a
// CacheReader, so access to it is buffered.  OffsetReaders are "windows" on
// existing readers, where the offset is relative to an offset in the
// underlying reader (useful for accessing elf sections by section-relative
// offsets, for example.)
// There are also compressed readers for zlib and lzma-encoded content embedded
// in files.
//
// Example: ELF binaries can contain a ".gnu_debugdata" section, that is an
// LZMA encoded ELF image itself, that contains a symbol table section
// When accessing that symbol table, we'll read it from a stack of readers like
// this:
//
// FileReader (for ELF image)
// CacheReader (for performance)
// OffsetReader (for .gnu_debugdata section)
// LzmaReader (to decompress the .gnu_debugdata, and give the plain ELF image)
// OffsetReader (for .symtab in the nested ELF image)

template <typename T, typename Iter> static inline std::pair<T, size_t> readleb128(Iter start) {
   static_assert(CHAR_BIT == 8);
   T result = 0;
   unsigned shift = 0;
   unsigned char byte;
   for (auto it = start;; ++it) {
      byte = *it;
      result |= T(byte & 0x7f) << shift;
      shift += 7;
      if ((byte & 0x80) == 0) {
         if constexpr (std::is_signed_v<T>) {
            using U_T = typename std::make_unsigned<T>::type;
            if (shift < sizeof(T) * CHAR_BIT && (byte & 0x40))
               result |= ~U_T(0) << shift;
         }
         return { result, it - start + 1 };
      }
   };
}


class Reader : public std::enable_shared_from_this<Reader> {
    Reader(const Reader &);
public:
    using Off = unsigned long;
    Reader() {}
    virtual ~Reader() {}

    // Read a consecutive sequence of objects type Obj starting at Offset.
    template <typename Obj> void readObj(Off offset, Obj *object, size_t count = 1) const;

    // Helper to read a single object.
    template <typename Obj> Obj readObj(Off offset) const;
    // read a sequence of count bytes at offset off. May give a short return.
    virtual size_t read(Off off, size_t count, char *ptr) const = 0;

    // read a LEB128 encoded integer.
    virtual std::pair<uintmax_t, size_t> readULEB128(Off off) const;
    virtual std::pair<intmax_t, size_t> readSLEB128(Off off) const;

    // describe this reader.
    virtual void describe(std::ostream &os) const = 0;

    // give the name of the file we are eventually reading
    virtual std::string filename() const = 0;

    // read a text string at an offset
    virtual std::string readString(Off offset) const;

    virtual Off size() const = 0;
    typedef std::shared_ptr<Reader> sptr;
    typedef std::shared_ptr<const Reader> csptr;
    virtual csptr view(const std::string &name, Off start, Off length=std::numeric_limits<Off>::max()) const;
};

static inline std::ostream &operator << (std::ostream &os, const Reader &reader)
{
    reader.describe(os);
    return os;
}

template <typename Obj> void
Reader::readObj(Off offset, Obj *object, size_t count) const
{
    if (count == 0)
        return;
    size_t rc = read(offset, count * sizeof *object, (char *)object);
    if (rc != count * sizeof *object)
        throw Exception() << "incomplete object read from " << *this
           << " at offset " << offset
           << " for " << count << " bytes";
}

template <typename Obj> Obj
Reader::readObj(Off offset) const
{
   Obj t;
   readObj(offset, &t);
   return t;
}

// Reader implementations
class FileReader : public Reader {
    std::string name;
    int file;
    mutable Off fileSize;
public:
    virtual size_t read(Off off, size_t count, char *ptr) const override ;
    FileReader(std::string name_);
    FileReader(std::string name_, Off minsize);
    ~FileReader();
    void describe(std::ostream &os) const  override { os << name; }
    std::string filename() const override { return name; }
    Off size() const override;
};

// Reader implementations

class CacheReader final : public Reader {
    struct CacheEnt {
        std::string value;
        bool isNew;
        CacheEnt() : isNew(true) {}
    };
    Reader::csptr upstream;
    mutable std::unordered_map<Off, CacheEnt> stringCache;
    static const size_t PAGESIZE = 256;
    static const size_t MAXPAGES = 16;
    class Page {
        Page(const Page &) = delete;
    public:
        Off offset;
        size_t len;
        char data[PAGESIZE];
        Page() {};
        void load(const Reader &r, Off offset_);
    };
    mutable std::list<Page *> pages;
    Page *getPage(Off pageoff) const;
public:
    void flush();
    virtual size_t read(Off off, size_t count, char *ptr) const override;
    virtual void describe(std::ostream &os) const override {
        // this must be the same as the underlying stream: we sometimes rely on the
        // FileReader's filename
        os << *upstream;
    }
    CacheReader(Reader::csptr upstream_);
    std::string readString(Off off) const override;
    ~CacheReader();
    Off size() const override { return upstream->size(); }
    std::string filename() const override { return upstream->filename(); }
};

class MemReader : public Reader {
protected:
    std::string descr;
public:
    size_t len;
    const void *data;
    virtual size_t read(Off off, size_t count, char *ptr) const override;
    MemReader(const std::string &, size_t, const void *);
    void describe(std::ostream &) const override;
    Off size() const override { return len; }
    std::string filename() const override { return "in-memory"; }
    std::string readString(Off offset) const override;
    virtual csptr view(const std::string &name, Off start, Off length=std::numeric_limits<Off>::max()) const override;
    virtual std::pair<uintmax_t, size_t> readULEB128(Off off) const override;
    virtual std::pair<intmax_t, size_t> readSLEB128(Off off) const override;
};

class MmapReader final : public MemReader {
public:
    MmapReader(const std::string &name_);
    ~MmapReader();
    std::string filename() const override { return descr; }
};

class NullReader final : public Reader {
public:
    virtual size_t read(Off, size_t, char *) const override {
        throw Exception() << " read from null reader";
    }
    void describe(std::ostream &os) const override {
        os << "empty reader";
    }
    Off size() const override { return 0; }
    std::string filename() const override { return "nowhere"; }
};

class OffsetReader final : public Reader {
    Reader::csptr upstream;
    Off offset;
    Off length;
public:
    std::string name;
    std::string readString(Off absoff) const override {
        return upstream->readString(absoff + offset);
    }
    virtual size_t read(Off off, size_t count, char *ptr) const override;
    OffsetReader(const std::string &name, Reader::csptr upstream_, Off offset_, Off length_ = std::numeric_limits<Off>::max());
    void describe(std::ostream &os) const override {
        os << name << "( range " << "[" << offset << "," << offset + length << "]" << " of " << *upstream << " )";
    }
    Off size() const override { return length; }
    std::string filename() const override { return upstream->filename(); }
};

Reader::csptr loadFile(const std::string &path);
Reader::csptr loadFile(const std::string &path, Reader::Off minsize);

// This allows a reader to provide an iterator over a sequence of objects of a
// given type. Given a reader r, we can use 
// for (const Foo &foo : ReaderArray<Foo>(r)) {
//   ...
// }
template <typename T>
struct ReaderArray {
   class iterator {
      const Reader *reader;
      T datum;
       void getitem();
   public:
      Reader::Off offset;
      T &operator *();
      iterator(const Reader *reader_) : reader(reader_),offset(reader->size()) { }
      iterator(const Reader *reader_, Reader::Off offset_) : reader(reader_),offset(offset_) {
          getitem();
      }
      bool operator == (const iterator &rhs) const { return offset == rhs.offset && reader == rhs.reader; }
      bool operator != (const iterator &rhs) const { return ! (*this == rhs); }
      size_t operator - (const iterator &rhs) const { return offset - rhs.offset; }
      iterator & operator++();
   };
   using const_iterator = iterator;
   const Reader &reader;
   size_t initialOffset;
   typedef T value_type;
   iterator begin() const { return iterator(&reader, initialOffset); }
   iterator end() const { return iterator(&reader); }
   ReaderArray(const Reader &reader_, size_t offset = 0) : reader(reader_), initialOffset(offset) {
      assert(reader.size() == std::numeric_limits<Reader::Off>::max() || reader.size() % sizeof (T) == 0);
   }
};

template <typename T> void ReaderArray<T>::iterator::getitem() {
    try {
        reader->readObj(offset, &datum);
    }
    catch (const Exception &ex) {
        // if we hit an error, just set our size to the reader size. We'll now
        // compare equal to end()
        offset = reader->size();
    }

}

template<typename T>
typename ReaderArray<T>::iterator &ReaderArray<T>::iterator::operator ++() {
    offset += sizeof (T);
    if (offset != reader->size())
        getitem();
    return *this;

}

template <typename T> T &ReaderArray<T>::iterator::operator *() {
   return datum;
}

}

#endif
