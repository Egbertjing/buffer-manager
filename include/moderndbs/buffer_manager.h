#ifndef INCLUDE_MODERNDBS_BUFFER_MANAGER_H
#define INCLUDE_MODERNDBS_BUFFER_MANAGER_H

#include "moderndbs/file.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <assert.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include <shared_mutex>
#include <map>
namespace moderndbs {

enum FrameState {
   empty, // data not loaded yet
   clean, // data loaded, unmodified
   dirty // data loaded, has changed
};

class BufferFrame {
   private:
   friend class BufferManager;
   // TODO: add your implementation here
   // ID of the page
   uint64_t page_id;
   // max size of data
   size_t page_size;
   // ID of the segment
   uint16_t segment_id;

   size_t pageOffsetInFile = 0;
   // current state of the page (empty (not loaded), clean, dirty)
   FrameState state;
   // Data contained in this page
   char* data;

   // Read/Write lock for a single bufferframe
   // This lock is used to implement the exclusive or shared access of the frame
   pthread_rwlock_t frameLock;

   void readPage();
   void writePage();

   public:
   /// Returns a pointer to this page's data.

   BufferFrame(uint64_t page_id, size_t page_size, uint16_t segment_id);
   ~BufferFrame();

   // Give access to the content of the buffered page
   char* get_data();
   // Read page from disk

   void setDirty();
   void flush();

   uint64_t getPageID() const {
      return page_id;
   }

   bool lockWrite(bool blocking);
   bool lockRead(bool blocking);
   void unlock();
};

class buffer_full_error
   : public std::exception {
   public:
   [[nodiscard]] const char* what() const noexcept override {
      return "buffer is full";
   }
};

class BufferManager {
   private:
   // TODO: add your implementation here
   // page size
   size_t page_size;
   // maximum number of bufferd pages
   size_t page_count;
   // has table mutex
   std::mutex mtx;
   // mutable std::shared_mutex vector_mutex;
   // Hashmap that maps page ids to the corresponding buffer frame
   std::unordered_map<uint64_t, BufferFrame*> bufferFrameMap;
   // Store file descriptors of opened segment files
   std::unordered_map<uint16_t, int> segmentsidMap;
   // FIFO replacement strategy
   std::vector<BufferFrame*> fifo;
   // LRU replacement strategy
   std::vector<BufferFrame*> lru;

   public:
   BufferManager(const BufferManager&) = delete;
   BufferManager(BufferManager&&) = delete;
   BufferManager& operator=(const BufferManager&) = delete;
   BufferManager& operator=(BufferManager&&) = delete;
   /// Constructor.
   /// @param[in] page_size  Size in bytes that all pages will have.
   /// @param[in] page_count Maximum number of pages that should reside in
   //                        memory at the same time.
   BufferManager(size_t page_size, size_t page_count);

   /// Destructor. Writes all dirty pages to disk.
   ~BufferManager();

   /// Returns a reference to a `BufferFrame` object for a given page id. When
   /// the page is not loaded into memory, it is read from disk. Otherwise the
   /// loaded page is used.
   /// When the page cannot be loaded because the buffer is full, throws the
   /// exception `buffer_full_error`.
   /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
   /// `unfix_page()`.
   /// @param[in] page_id   Page id of the page that should be loaded.
   /// @param[in] exclusive If `exclusive` is true, the page is locked
   ///                      exclusively. Otherwise it is locked
   ///                      non-exclusively (shared).
   BufferFrame& fix_page(uint64_t page_id, bool exclusive);

   /// Takes a `BufferFrame` reference that was returned by an earlier call to
   /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
   /// written back to disk eventually.
   void unfix_page(BufferFrame& page, bool is_dirty);

   /// Returns the page ids of all pages (fixed and unfixed) that are in the
   /// FIFO list in FIFO order.
   /// Is not thread-safe.
   [[nodiscard]] std::vector<uint64_t> get_fifo_list() const;

   /// Returns the page ids of all pages (fixed and unfixed) that are in the
   /// LRU list in LRU order.
   /// Is not thread-safe.
   [[nodiscard]] std::vector<uint64_t> get_lru_list() const;

   /// Returns the segment id for a given page id which is contained in the 16
   /// most significant bits of the page id.
   static constexpr uint16_t get_segment_id(uint64_t page_id) {
      return page_id >> 48;
   }

   /// Returns the page id within its segment for a given page id. This
   /// corresponds to the 48 least significant bits of the page id.
   static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
      return page_id & ((1ull << 48) - 1);
   }

   // Check if we have space for another frame
   bool isFrameAvailable();
};

} // namespace moderndbs

#endif
