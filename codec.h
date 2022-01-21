#ifndef __HASHDATA_CODEC__
#define __HASHDATA_CODEC__


#include <string>

#include "sqlite/sqliteInt.h"
#include "sqlite/sqlite3.h"

#include "hash3.h"


inline std::string format(const char *form, ...) {
    char buf[200];
    va_list args;

    va_start(args, form);
    int size = vsnprintf(buf, sizeof(buf), form, args);
    va_end(args);

    if (size >= 0 && size < sizeof(buf)) {
        return std::string(buf, size);
    }

    if (size < 0) {
        throw std::exception();
    }

    std::string s;
    s.resize(size + 1);
    va_start(args, form);
    size = vsnprintf(&s[0], s.size(), form, args);
    va_end(args);
    if (size < 0 || size >= s.size()) {
        throw std::exception();
    }

    s.resize(size);
    return s;
}


struct page_checksum_codec_t {
    page_checksum_codec_t(std::string const &filename) : pageSize(0), reserveSize(0), filename(filename),
                                                         silent(false) {}

    int pageSize;
    int reserveSize;
    std::string filename;
    bool silent;

    struct sum_type_t {
        bool operator==(const sum_type_t &rhs) const { return part1 == rhs.part1 && part2 == rhs.part2; }

        bool operator!=(const sum_type_t &rhs) const { return part1 != rhs.part1 || part2 != rhs.part2; }

        uint32_t part1;
        uint32_t part2;

        std::string toString() { return format("0x%08x%08x", part1, part2); }
    };

    // Calculates and then either stores or verifies a checksum.
    // The checksum is read/stored at the end of the page buffer.
    // Page size is passed in as pageLen because this->pageSize is not always appropriate.
    // If write is true then the checksum is written into the page and true is returned.
    // If write is false then the checksum is compared to the in-page sum and the return value
    // is whether or not the checksums were equal.
    bool checksum(Pgno pageNumber, void *data, int pageLen, bool write) {

        char *pData = (char *) data;
        int dataLen = pageLen - sizeof(sum_type_t);
        sum_type_t sum;
        sum_type_t *pSumInPage = (sum_type_t *) (pData + dataLen);

        // Write sum directly to page or to sum variable based on mode
        sum_type_t *sumOut = write ? pSumInPage : &sum;
        sumOut->part1 = pageNumber; //DO NOT CHANGE
        sumOut->part2 = 0x5ca1ab1e;
        hashlittle2(pData, dataLen, &sumOut->part1, &sumOut->part2);

        // Verify if not in write mode
        if (!write && (sum != *pSumInPage)) {
            return false;
        }

        return true;
    }

    static void *codec(void *vpSelf, void *data, Pgno pageNumber, int op) {
        page_checksum_codec_t *self = (page_checksum_codec_t *) vpSelf;

        // Page write operations are 6 for DB page and 7 for journal page
        bool write = (op == 6 || op == 7);
        // Page read is operation 3, which must be the operation if it's not a write.

        // Page 1 is special.  It contains the database configuration including Page Size and Reserve Size.
        // SQLite can't get authoritative values for these things until the Pager Codec has validated (and
        // potentially decrypted) Page 1 itself, so it can't tell the Pager Codec what those things are before
        // Page 1 is handled.  It will guess a Page Size of SQLITE_DEFAULT_PAGE_SIZE, and a Reserve Size based
        // on the pre-verified (and perhaps still encrypted) header in the Page 1 data that it will then pass
        // to the Pager Codec.
        //
        // So, Page 1 must be written and verifiable as a SQLITE_DEFAULT_PAGE_SIZE sized page as well as
        // the actual configured page size for the database, if it is larger.  A configured page size lower
        // than the default (in other words 512) results in undefined behavior.
        if (pageNumber == 1) {
            if (write && self->pageSize > SQLITE_DEFAULT_PAGE_SIZE) {
                self->checksum(pageNumber, data, SQLITE_DEFAULT_PAGE_SIZE, write);
            }
        } else {
            // For Page Numbers other than 1, reserve size must be the size of the checksum.
            if (self->reserveSize != sizeof(sum_type_t)) {
                return NULL;
            }
        }

        if (!self->checksum(pageNumber, data, self->pageSize, write))
            return NULL;

        return data;
    }

    static void sizeChange(void *vpSelf, int new_pageSize, int new_reserveSize) {
        page_checksum_codec_t *self = (page_checksum_codec_t *) vpSelf;
        self->pageSize = new_pageSize;
        self->reserveSize = new_reserveSize;
    }

    static void free(void *vpSelf) {
        page_checksum_codec_t *self = (page_checksum_codec_t *) vpSelf;
        delete self;
    }
};


#endif /* __HASHDATA_CODEC__ */