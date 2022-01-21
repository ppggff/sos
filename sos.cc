#include <cstdint>
#include <string>
#include <iostream>

#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>

#include <arpa/inet.h>

#include <sstream>
#include <vector>


#define SQLITE_THREADSAFE 0  // also in sqlite3.amalgamation.c!

#include "hash3.h"

extern "C" {
#include "sqlite/sqliteInt.h"
}

#if SQLITE_THREADSAFE == 0
#define sqlite3_mutex_enter(x)
#define sqlite3_mutex_leave(x)
#endif


#include "codec.h"


struct index_leaf_page_header_t {
    uint8_t flag;                // A value of 10 (0x0a) means the page_t is a leaf index b-tree page_t.
    uint16_t free_block_offset;  // start of the first freeblock on the page_t, or is zero if there are no freeblocks.
    uint16_t number_of_cell;     // number of index_leaf_cells_t on the page_t
    uint16_t cell_region_offset;        // the start of the cell content area. A zero value for this integer is interpreted as 65536.
    int8_t number_of_free_bytes; // number of fragmented free bytes within the cell content area.

    std::string to_string() const {
        std::stringstream ss;
        ss << " flag: " << std::showbase << std::hex << (int) flag << std::dec
           << " free_block_offset: " << free_block_offset
           << " number_of_cell: " << number_of_cell
           << " cell_region_offset: " << cell_region_offset
           << " number_of_free_bytes: " << (int) number_of_free_bytes;
        return ss.str();
    }
};

struct index_leaf_cells_t {
    std::vector<uint16_t> offsets;
    const char *cell_region_base = nullptr;

    std::string to_string() {
        std::stringstream ss;
        ss << "cell count: " << offsets.size() << " ";

        for (int i = 0; i < 5 && i < offsets.size(); ++i) {
            ss << "cell " << i << ": " << offsets[i] << ", ";
        }

        return ss.str();
    }
};


/*
 * Index B-Tree Leaf Cell (header 0x0a):
 * A varint which is the total number of bytes of key payload, including any overflow
 * The initial portion of the payload that does not spill to overflow pages.
 * A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
 */
struct payload_t {
    uint64_t payload_body_size = 0;
    std::vector<char> payload;
    std::vector<int32_t> overflow_pages;
    bool valid = true;

    std::string to_string() {
        std::stringstream ss;
        ss << "payload body size: " << payload_body_size << ", " << payload.data();
        return ss.str();
    }
};

struct index_leaf_page_t {
    const char *base;
    const char *position;
    const int64_t pno = 0;

    index_leaf_page_t(const char *base, int64_t pno) : base(base), pno(pno) {
        position = base + ((pno - 1) * 4096);
    };

    bool is_index_leaf() const {
        return *position == 0x0a;
    }

    index_leaf_page_header_t get_page_header() const {
        index_leaf_page_header_t header{};
        header.flag = *position;
        header.free_block_offset = htons(*(uint16_t *) (position + 1));
        header.number_of_cell = htons(*(uint16_t *) (position + 3));
        header.cell_region_offset = htons(*(uint16_t *) (position + 5));
        header.number_of_free_bytes = *(int8_t *) (position + 7);

        return header;
    }

    index_leaf_cells_t get_cells(const index_leaf_page_header_t &header) const {
        index_leaf_cells_t cs;

        cs.cell_region_base = position + header.cell_region_offset;
        cs.offsets.resize(header.number_of_cell);
        const char *off = position + 8;  // The b-tree page header is 8 bytes in size for leaf pages

        for (int i = 0; i < header.number_of_cell; ++i) {
            cs.offsets[i] = htons(*(int16_t *) (off + (i * 2)));
        }

        return std::move(cs);
    }

    payload_t get_payload(index_leaf_cells_t &cells, int index) const {
        payload_t payload{};

        uint16_t cell_offset = cells.offsets[index];
        const char *payload_header_position = position + cell_offset;
        // A varint which is the total number of bytes of key payload, including any overflow
        const char *payload_body_position =
                payload_header_position + sqlite3GetVarint((const unsigned char *) payload_header_position,
                                                           (u64 *) &payload.payload_body_size);

        if (payload.payload_body_size > 4096) {
            std::cout << "skip payload of cell " << index << ", since its size " << payload.payload_body_size
                      << " is too large" << std::endl;
            payload.valid = false;
            return std::move(payload);
        }

        payload.payload.resize(payload.payload_body_size);

        memcpy(payload.payload.data(), payload_body_position, payload.payload_body_size);
        // TODO: overflow ?

        return std::move(payload);
    }
};

struct database_t {
    int fd = 0;
    int64_t size = 0;
    const char *base = nullptr;

    int64_t get_page_size() const {
        return size / 4096;
    }

    index_leaf_page_t get_page(int64_t pno) const {
        return index_leaf_page_t{base, pno};
    }
};


struct metrics_t {
    uint32_t pages = 0;
    uint32_t skip_pages = 0;

    uint64_t cells = 0;
    uint64_t bytes = 0;

    std::string to_string() const {
        std::stringstream ss;
        ss << "pages: " << pages << ", skip pages: " << skip_pages << ", cells: " << cells << ", bytes: " << bytes
           << std::endl;
        return ss.str();
    }
};

struct restore_context_t {
    std::string filename = "template.sqlite";
    sqlite3 *db;
    Btree *btree;
    BtCursor *cursor;
    page_checksum_codec_t *codec;
    KeyInfo keyInfo;

    metrics_t metrics;
};


void check_error(const std::string &op, int result) {
    if (result) {
        std::cout << "sqlite failure, operation: " << op << " message: " << sqlite3ErrStr(result);
        exit(1);
    }
}

struct statement_t {
    restore_context_t &ctx;
    sqlite3_stmt *stmt;

    statement_t(restore_context_t &ctx, const char *sql)
            : ctx(ctx), stmt(nullptr) {
        check_error("prepare", sqlite3_prepare_v2(ctx.db, sql, -1, &stmt, nullptr));
    }

    ~statement_t() {
        try {
            check_error("finalize", sqlite3_finalize(stmt));
        } catch (...) {
        }
    }

    statement_t &execute() {
        int r = sqlite3_step(stmt);

        if (r == SQLITE_ROW) {
            check_error("execute called on statement that returns rows", r);
        }

        if (r != SQLITE_DONE) {
            check_error("execute", r);
        }

        return *this;
    }

    bool next_row() const {
        int r = sqlite3_step(stmt);
        if (r == SQLITE_ROW) {
            return true;
        }

        if (r == SQLITE_DONE) {
            return false;
        }

        check_error("next_row", r);
        return true;
    }
};


void begin_restore(restore_context_t &ctx) {
    int result = sqlite3_open_v2(ctx.filename.data(), &ctx.db, SQLITE_OPEN_READWRITE, nullptr);
    check_error("open", result);

    ctx.btree = ctx.db->aDb[0].pBt;
    int r = sqlite3_test_control(SQLITE_TESTCTRL_RESERVE, ctx.db, sizeof(page_checksum_codec_t::sum_type_t));

    if (r != 0) {
        std::cout << "sqlite3_test_control() failed";
        exit(1);
    }

    // Always start with a new pager codec with default options.
    ctx.codec = new page_checksum_codec_t(ctx.filename);
    sqlite3BtreePagerSetCodec(ctx.btree, page_checksum_codec_t::codec, page_checksum_codec_t::sizeChange,
                              page_checksum_codec_t::free, ctx.codec);

    sqlite3_extended_result_codes(ctx.db, 1);

    statement_t(ctx, "PRAGMA journal_mode = WAL").next_row();
    statement_t(ctx, "PRAGMA synchronous = NORMAL").execute(); // OFF, NORMAL, FULL
    statement_t(ctx, "PRAGMA auto_vacuum = NONE").execute();
    statement_t(ctx, "PRAGMA wal_autocheckpoint = 1").next_row();


    ctx.keyInfo.db = ctx.db;
    ctx.keyInfo.enc = ctx.db->aDb[0].pSchema->enc;
    ctx.keyInfo.aColl[0] = ctx.db->pDfltColl;
    ctx.keyInfo.aSortOrder = 0;
    ctx.keyInfo.nField = 1;

    ctx.cursor = static_cast<BtCursor *>(malloc(sqlite3BtreeCursorSize()));
}


void checkpoint(restore_context_t &ctx, bool restart) {
    while (true) {
        int rc = sqlite3_wal_checkpoint_v2(ctx.db, 0, restart ? SQLITE_CHECKPOINT_RESTART : SQLITE_CHECKPOINT_FULL,
                                           nullptr, nullptr);
        if (!rc) {
            break;
        }
        if ((sqlite3_errcode(ctx.db) & 0xff) == SQLITE_BUSY) {
            sqlite3_sleep(10);
        } else
            check_error("checkpoint", rc);
    }
}


void full_checkpoint(restore_context_t &ctx) {
    checkpoint(ctx, false);
    checkpoint(ctx, true);
}

void restore_page(restore_context_t &ctx, index_leaf_page_t &p, index_leaf_page_header_t &header,
                  index_leaf_cells_t &cells) {
    check_error("BtreeBeginTrans", sqlite3BtreeBeginTrans(ctx.btree, true));

    sqlite3BtreeCursorZero(ctx.cursor);
    check_error("BtreeCursor", sqlite3BtreeCursor(ctx.btree, 3, true, &ctx.keyInfo, ctx.cursor));

    ctx.metrics.cells += header.number_of_cell;

    for (int i = 0; i < header.number_of_cell; ++i) {
        payload_t payload = p.get_payload(cells, i);

        if (!payload.valid || payload.payload_body_size == 0) {
            continue;
        }

        ctx.metrics.bytes += payload.payload.size();

        // for index type btree, payload is the (fdb encoded) key, no value here
        check_error("BtreeBeginTrans", sqlite3BtreeInsert(
                ctx.cursor, payload.payload.data(), payload.payload.size(),
                nullptr, 0, 0, 0, 0));
    }

    check_error("BtreeCloseCursor", sqlite3BtreeCloseCursor(ctx.cursor));
    check_error("BtreeCommit", sqlite3BtreeCommit(ctx.btree));

    std::cout << "Committed" << std::endl;

    full_checkpoint(ctx);
}

void complete_restore(restore_context_t &ctx) {
    full_checkpoint(ctx);

    check_error("sqlite3_close", sqlite3_close(ctx.db));
    ctx.db = nullptr;
}

/*
 * Index B-Tree Leaf Cell (header 0x0a):
 *    A varint which is the total number of bytes of key payload, including any overflow
 *    The initial portion of the payload that does not spill to overflow pages.
 *    A 4-byte big-endian integer page_t number for the first page_t of the overflow page_t list - omitted if all payload fits on the b-tree page_t.
 */
void dump_index_leaf_page(restore_context_t &ctx, const database_t &db, index_leaf_page_t &p) {
    ctx.metrics.pages += 1;

    index_leaf_page_header_t header = p.get_page_header();
    std::cout << "page: " << p.pno << ", " << header.to_string() << std::endl;

    index_leaf_cells_t cells = p.get_cells(header);
    std::cout << cells.to_string() << std::endl;

    restore_page(ctx, p, header, cells);
}

void open_and_dump(restore_context_t &ctx, const std::string &file, int start_page) {
    struct stat st{};

    int rc = stat(file.data(), &st);
    if (rc != 0) {
        std::cout << "Cannot stat file " << file << std::endl;
        std::exit(1);
    }

    database_t db{};
    db.fd = open(file.data(), O_RDONLY);

    if (db.fd < 0) {
        std::cout << "Cannot open file " << file << std::endl;
        std::exit(1);
    }

    db.size = st.st_size;
    db.base = (const char *) mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, db.fd, 0);

    // loop all pages, page no start from 1
    for (int i = start_page; i < db.get_page_size() + 1; ++i) {
        index_leaf_page_t p = db.get_page(i);

        if (!p.is_index_leaf()) {
            ctx.metrics.skip_pages += 1;
            continue;
        }

        dump_index_leaf_page(ctx, db, p);
    }
}

int main(int argc, const char **argv) {
    if (argc < 4) {
        std::cout << "Invalid input" << std::endl;
        std::exit(1);
    }

    char *end;
    int start_page = (int) strtol(argv[3], &end, 10);

    if (end == argv[3] || *end != 0 || start_page < 2) {
        std::cout << "Invalid start page " << argv[3] << std::endl;
        std::exit(1);
    }

    restore_context_t ctx{argv[2]};

    begin_restore(ctx);
    open_and_dump(ctx, argv[1], start_page);
    complete_restore(ctx);

    std::cout << ctx.metrics.to_string();
}
