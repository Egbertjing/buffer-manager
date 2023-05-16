#include "moderndbs/buffer_manager.h"

namespace moderndbs {

BufferFrame::BufferFrame(uint64_t page_id, size_t page_size, uint16_t segment_id)
   : page_id(page_id), page_size(page_size), segment_id(segment_id) {
   // Initially the data points to null and the state is empty
   pthread_rwlock_init(&frameLock, NULL);
   data = nullptr;
   pageOffsetInFile = (page_id & ((1ull << 48) - 1)) * page_size;
   state = FrameState::empty;
}

BufferFrame::~BufferFrame() {
   // Destroy the lock on the buffFrame
   pthread_rwlock_destroy(&frameLock);
   flush();
   if (data != nullptr) {
      free(data);
      data = nullptr;
   }
}

char* BufferFrame::get_data() {
   // TODO: add your implementation here
   if (state == FrameState::empty) {
      readPage();
   }
   return data;
}

// Read page from disk
void BufferFrame::readPage() {
   // read the data from the file
   // get file name
   data = (char*) malloc(page_size);
   std::string str = std::to_string(segment_id);
   const char* file_name = str.c_str();

   // Open the file
   std::unique_ptr<File> fileptr = nullptr;
   try {
      fileptr = File::open_file(file_name, File::Mode::READ);
   } catch (const std::exception& e) {
      std::ofstream file(file_name);
      fileptr = File::open_file(file_name, File::Mode::READ);
   }
   // Read the data from the file
   fileptr->read_block(pageOffsetInFile, page_size, data);
   // Set the state to clean
   state = FrameState::clean;
}

// Write page to disk
void BufferFrame::writePage() {
   // get file name
   std::string str = std::to_string(segment_id);
   const char* file_name = str.c_str();
   // Open the file
   std::unique_ptr<File> fileptr = nullptr;
   try {
      fileptr = File::open_file(file_name, File::Mode::WRITE);
   } catch (const std::exception& e) {
      std::ofstream file(file_name);
      fileptr = File::open_file(file_name, File::Mode::WRITE);
   }
   // Write the data from the file
   fileptr->write_block(data, pageOffsetInFile, page_size);
   // Set the state to clean
   state = FrameState::clean;
}

// Flush frame to disk
void BufferFrame::flush() {
   // Writeback only if page has been modified
   if (state == FrameState::dirty)
      writePage();
}

void BufferFrame::setDirty() {
   state = FrameState::dirty;
}

// Lock frame for exclusive write
bool BufferFrame::lockWrite(bool exclusive) {
   if (!exclusive) {
      return pthread_rwlock_trywrlock(&frameLock) == 0;
   } else {
      return pthread_rwlock_wrlock(&frameLock) == 0;
   }
}
// Lock frame for read
bool BufferFrame::lockRead(bool exclusive) {
   if (!exclusive) {
      return pthread_rwlock_tryrdlock(&frameLock) == 0;
   } else {
      return pthread_rwlock_rdlock(&frameLock) == 0;
   }
}
// Release read or write
void BufferFrame::unlock() {
      pthread_rwlock_unlock(&frameLock);
}

BufferManager::BufferManager(size_t page_size, size_t page_count) : page_size(page_size), page_count(page_count) {
   // TODO: add your implementation here
   bufferFrameMap.reserve(page_count);
}

BufferManager::~BufferManager() {
   // TODO: add your implementation here
   for (auto& kv : bufferFrameMap) {
      delete (kv.second);
   }
   // close all opened file descriptors
   for (auto& kv : segmentsidMap)
      close(kv.second);
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
   // TODO: add your implementation here
   BufferFrame* frame = nullptr;

   // Lock the buffer manager
   // we need the lock as every page could be freed spontaneously
   mtx.lock();
   // Check if page is already in buffer, then just return existing frame (with rwlock)

   auto entry = bufferFrameMap.find(page_id);
   if (entry != bufferFrameMap.end()) {
      // Requested PageID is already in Buffer
      // Move the frame to the end of the lru
      lru.erase(std::remove(lru.begin(), lru.end(), entry->second), lru.end());
      lru.push_back(entry->second);
      fifo.erase(std::remove(fifo.begin(), fifo.end(), entry->second), fifo.end());

      frame = entry->second;

      // First try to lock the frame without block
      bool locked = false;
      if (exclusive) {
         locked = frame->lockWrite(false);
      } else {
         locked = frame->lockRead(false);
      }

      // Unlock the BufferManager inside lock
      // @TODO: do this earlier - race conditions!
      mtx.unlock();

      // If we could not lock it directly we wait for the unlock
      if (!locked) {
         if (exclusive) {
            locked = frame->lockWrite(true);
         } else {
            locked = frame->lockRead(true);
         }
      }
   } else {
      // Requested PageID was not in Buffer
      // Frame could not be found
      // Try to find a free slot || replace unfixed pages
      if (!isFrameAvailable()) {
         // Buffer does not contain free frames
         // Search a page that is not locked (framestate is not important)
         BufferFrame* replacementCandidate = nullptr;
         for (std::vector<BufferFrame*>::iterator it = fifo.begin(); it != fifo.end(); ++it) {
            // Just try to acquire an exclusive lock without block
            if ((*it)->lockWrite(false)) {
               replacementCandidate = *it;
               // Found replacement candidate with pageID
               break;
            }
         }

         if (replacementCandidate == nullptr) {
            // No replacement candidate found
            // Buffer is full and all pages are locked
            // Throw error
            mtx.unlock();
            throw buffer_full_error{};
         } else {
            // Write page to disk if dirty
            replacementCandidate->flush();
            bufferFrameMap.erase(replacementCandidate->getPageID());
            fifo.erase(std::remove(fifo.begin(), fifo.end(), replacementCandidate), fifo.end());
            delete replacementCandidate;
         }
      }

      // Create new frame
      // Creating new Frame with pageID
      frame = new BufferFrame(page_id, page_size, get_segment_id(page_id));
      auto result = bufferFrameMap.insert(std::make_pair(page_id, frame));
      fifo.push_back(result.first->second);
      // Frame is fresh in buffer -> Lock immediately
      if (exclusive) {
         frame->lockWrite(true);
      } else {
         frame->lockRead(true);
      }

      // Unlock BufferManager
      // @TODO: do this earlier - race conditions!
      mtx.unlock();
   }
   return *frame;
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
   // TODO: add your implementation here
   if (is_dirty) {
      page.setDirty();
   }
   page.unlock();
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
   // TODO: add your implementation here
   std::vector<uint64_t> fifoList;
   for (auto& frame : fifo) {
      fifoList.push_back(frame->getPageID());
   }
   return fifoList;
}

bool BufferManager::isFrameAvailable() {
   return bufferFrameMap.size() < page_count;
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
   // TODO: add your implementation here
   std::vector<uint64_t> lruList;
   for (auto& frame : lru) {
      lruList.push_back(frame->getPageID());
   }
   return lruList;
}

} // namespace moderndbs
