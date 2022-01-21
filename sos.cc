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

const uint64_t page_size = 4096;
const uint64_t reserved_page_size = 8;
const uint64_t usable_size = page_size - reserved_page_size;
const uint64_t max_local = ((usable_size - 12) * 64 / 255) - 23;
const uint64_t min_local = ((usable_size - 12) * 32 / 255) - 23;


struct index_page_header_t {
    uint8_t flag;                // A value of 10 (0x0a) means the page_t is a leaf index b-tree page_t.
    uint16_t free_block_offset;  // start of the first freeblock on the page_t, or is zero if there are no freeblocks.
    uint16_t number_of_cell;     // number of index_leaf_cells_t on the page_t
    uint16_t cell_region_offset;        // the start of the cell content area. A zero value for this integer is interpreted as 65536.
    int8_t number_of_free_bytes; // number of fragmented free bytes within the cell content area.
    // The four-byte page number at offset 8 is the right-most pointer.
    // This value appears in the header of interior b-tree pages only and is omitted from all other pages.
    uint32_t right_most_pointer = 0;


    std::string to_string() const {
        std::stringstream ss;
        ss << " flag: " << std::showbase << std::hex << (int) flag << std::dec
           << " free_block_offset: " << free_block_offset
           << " number_of_cell: " << number_of_cell
           << " cell_region_offset: " << cell_region_offset
           << " number_of_free_bytes: " << (int) number_of_free_bytes
           << " right_most_pointer: " << right_most_pointer;
        return ss.str();
    }
};

struct index_cells_t {
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

struct index_page_t {
    const char *base;
    const char *position;
    const int64_t pno = 0;

    index_page_t(const char *base, int64_t pno) : base(base), pno(pno) {
        position = base + ((pno - 1) * 4096);
    };

    bool is_index_leaf() const {
        return *position == 0x0a;
    }

    bool is_index_interior() const {
        return *position == 0x02;
    }

    index_page_header_t get_page_header() const {
        index_page_header_t header{};
        header.flag = *position;
        header.free_block_offset = htons(*(uint16_t *) (position + 1));
        header.number_of_cell = htons(*(uint16_t *) (position + 3));
        header.cell_region_offset = htons(*(uint16_t *) (position + 5));
        header.number_of_free_bytes = *(int8_t *) (position + 7);

        if (is_index_interior()) {
            header.right_most_pointer = ntohl(*(uint32_t *) (position + 8));
        }

        return header;
    }

    index_cells_t get_cells(const index_page_header_t &header, index_page_t &p) const {
        index_cells_t cs;

        cs.cell_region_base = position + header.cell_region_offset;
        cs.offsets.resize(header.number_of_cell);
        const char *off = position;

        if (p.is_index_leaf()) {
            off += 8;  // The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
        } else {
            off += 12;
        };

        for (int i = 0; i < header.number_of_cell; ++i) {
            cs.offsets[i] = htons(*(int16_t *) (off + (i * 2)));
        }

        return std::move(cs);
    }


    static uint64_t calculate_embed_payload_size(uint64_t payload_body_size) {
        uint64_t surplus = min_local + ((payload_body_size - min_local) % (usable_size - 4));

        if (surplus <= max_local) {
            return surplus;
        } else {
            return min_local;
        }
    }

    /*
     * The first four bytes of each overflow page are a big-endian integer
     * which is the page number of the next page in the chain,
     * or zero for the final page in the chain.
     *
     * The fifth byte through the last usable byte are used to hold overflow content.
     */
    void loop_overflow_pages(payload_t &payload, uint64_t done) const {
        int overflow_page_id = payload.overflow_pages[0];

        while (overflow_page_id) {
            const char *next_page_position = this->base + ((overflow_page_id - 1) * 4096);
            // XXX: sanity heck
            overflow_page_id = htonl(*(uint32_t *) next_page_position);
            uint64_t todo = payload.payload.size() - done;
            todo = todo < usable_size - 4 ? todo : usable_size - 4;

            memcpy(payload.payload.data() + done, next_page_position + 4, todo);
            done += todo;
        }
    }

    payload_t get_payload(index_cells_t &cells, int index) const {
        payload_t payload{};

        uint16_t cell_offset = cells.offsets[index];
        const char *payload_header_position = position + cell_offset;
        const char *payload_body_position = payload_header_position;

        // payload.payload_body_size: A varint which is the total number of bytes of key payload, including any overflow

        if (is_index_interior()) {
            // A 4-byte big-endian page number which is the left child pointer.
            payload_body_position += 4;
            payload_body_position += sqlite3GetVarint((const unsigned char *) (payload_header_position + 4),
                                                      (u64 *) &payload.payload_body_size);
        } else {
            payload_body_position += sqlite3GetVarint((const unsigned char *) payload_header_position,
                                                      (u64 *) &payload.payload_body_size);
        }

        uint64_t max_embed_payload_size = calculate_embed_payload_size(payload.payload_body_size);

        if (payload.payload_body_size > max_embed_payload_size) {
            // overflow
            payload.payload.resize(payload.payload_body_size);
            memcpy(payload.payload.data(), payload_body_position, max_embed_payload_size);

            int overflow_page_id = htonl(*(uint32_t *) (payload_body_position + max_embed_payload_size));
            payload.overflow_pages.push_back(overflow_page_id);
            loop_overflow_pages(payload, max_embed_payload_size);
        } else {
            payload.payload.resize(payload.payload_body_size);
            memcpy(payload.payload.data(), payload_body_position, payload.payload_body_size);
        }

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

    index_page_t get_page(int64_t pno) const {
        return index_page_t{base, pno};
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

    int start_page = 2;

    int pages_in_transaction = 0;
    int pages_per_transaction = 1024;

    int transaction_in_checkpoint = 0;
    int transaction_per_checkpoint = 10;

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
    ctx.transaction_in_checkpoint = 0;
    checkpoint(ctx, false);
    checkpoint(ctx, true);

    std::cout << "Checkpoint Done" << std::endl;
}

void start_transaction(restore_context_t &ctx) {
    if (ctx.pages_in_transaction > 0) {
        // transaction already started
        ctx.pages_in_transaction += 1;
    } else {
        ctx.pages_in_transaction = 1;
        check_error("BtreeBeginTrans", sqlite3BtreeBeginTrans(ctx.btree, true));

        sqlite3BtreeCursorZero(ctx.cursor);
        check_error("BtreeCursor", sqlite3BtreeCursor(ctx.btree, 3, true, &ctx.keyInfo, ctx.cursor));
    }
}

void commit_transaction(restore_context_t &ctx, index_page_t &p) {
    if (ctx.pages_in_transaction > ctx.pages_per_transaction) {
        // transaction already started
        ctx.pages_in_transaction = 0;

        check_error("BtreeCloseCursor", sqlite3BtreeCloseCursor(ctx.cursor));
        check_error("BtreeCommit", sqlite3BtreeCommit(ctx.btree));

        std::cout << "Committed page " << p.pno << std::endl;

        ctx.transaction_in_checkpoint += 1;

        if (ctx.transaction_in_checkpoint > ctx.transaction_per_checkpoint) {
            full_checkpoint(ctx);
        }
    }
}

void restore_page(restore_context_t &ctx, index_page_t &p, index_page_header_t &header,
                  index_cells_t &cells) {
    start_transaction(ctx);

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

    commit_transaction(ctx, p);
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
void dump_index_page(restore_context_t &ctx, const database_t &db, index_page_t &p) {
    ctx.metrics.pages += 1;

    index_page_header_t header = p.get_page_header();
    std::cout << "page: " << p.pno << ", " << header.to_string() << std::endl;

    index_cells_t cells = p.get_cells(header, p);
    std::cout << cells.to_string() << std::endl;
    restore_page(ctx, p, header, cells);
}

void open_and_dump(restore_context_t &ctx, const std::string &file) {
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
    for (int i = ctx.start_page; i < db.get_page_size() + 1; ++i) {
        index_page_t p = db.get_page(i);

        if (!p.is_index_leaf() && !p.is_index_interior()) {
            ctx.metrics.skip_pages += 1;
            continue;
        }

        dump_index_page(ctx, db, p);
    }
}

int main(int argc, const char **argv) {
    if (argc < 4) {
        std::cout << "Version: 0.2.0" << std::endl
                  << "Usage:" << std::endl
                  << "  bin/sos <start_page_no> [pages_per_transaction] [transaction_per_checkpoint]" << std::endl
                  << "    " << "start_page_no: Start page number，must >=2" << std::endl
                  << "    " << "pages_per_transaction: pages per transaction interval, default 1024" << std::endl
                  << "    " << "transaction_per_checkpoint: transaction per checkpoint interval, default 10"
                  << std::endl;

        std::exit(1);
    }

    restore_context_t ctx{argv[2]};

    char *end;
    ctx.start_page = (int) strtol(argv[3], &end, 10);

    if (end == argv[3] || *end != 0 || ctx.start_page < 2) {
        std::cout << "Invalid start page " << argv[3] << std::endl;
        std::exit(1);
    }

    if (argc == 5) {
        ctx.pages_per_transaction = (int) strtol(argv[4], &end, 10);

        if (end == argv[4] || *end != 0 || ctx.pages_per_transaction < 1) {
            std::cout << "Invalid pages per checkpoint " << argv[4] << std::endl;
            std::exit(1);
        }
    }

    if (argc == 6) {
        ctx.transaction_per_checkpoint = (int) strtol(argv[5], &end, 10);

        if (end == argv[5] || *end != 0 || ctx.transaction_per_checkpoint < 1) {
            std::cout << "Invalid transaction per transaction " << argv[5] << std::endl;
            std::exit(1);
        }
    }


    begin_restore(ctx);
    open_and_dump(ctx, argv[1]);
    complete_restore(ctx);

    std::cout << ctx.metrics.to_string();
}
