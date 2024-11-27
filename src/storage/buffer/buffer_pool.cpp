#include "duckdb/storage/buffer/buffer_pool.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

S3FifoNode::S3FifoNode(weak_ptr<BlockHandle> handle_p) : handle(std::move(handle_p)) {
	D_ASSERT(!handle.expired());
}

typedef duckdb_moodycamel::ConcurrentQueue<S3FifoNode> fifo_queue_t;

//! Struct for the S3 FIFO queue
struct S3FifoQueue {
public:
	explicit S3FifoQueue(const FileBufferType file_buffer_type_p, idx_t maximum_memory)
	    : file_buffer_type(file_buffer_type_p) {
			idx_t gigabytes = ((maximum_memory / 1024) / 1024) / 1024;
			PROBATIONARY_QUEUE_SIZE = 1024;
			MAIN_QUEUE_SIZE = (4096 * gigabytes * 2) / 3;
			GHOST_QUEUE_SIZE = (4096 * gigabytes) / 3;
	}

	//! 3 static queues for S3 FIFO
	fifo_queue_t probationary_queue;
	fifo_queue_t main_queue;
	fifo_queue_t ghost_queue;
	
	//! The type of the buffers in this queue
	const FileBufferType file_buffer_type;

	//! Inserts a new node to the FIFO queue
	void QueueInsert(shared_ptr<BlockHandle> handle);
	//! Tries to dequeue an element, but only after acquiring the purge queue lock.
	bool TryDequeueWithLock(S3FifoNode &node);
	//! Evicts a node from the FIFO queue
	bool Evict();

private:
	//! Insert to the queues
	void ProbationaryQueueInsert(S3FifoNode &&node);
	void MainQueueInsert(S3FifoNode &&node, uint8_t default_accesses=0);
	void GhostQueueInsert(S3FifoNode &&node);

	//! Evict from the queues
	bool ProbationaryQueueEvict();
	bool MainQueueEvict();
	bool GhostQueueEvict();

private:
	//! TODO: Set these based on memory limit and block sizes
	//! TODO: Make static constexpr?
	//! S3 Queue Sizes
	idx_t PROBATIONARY_QUEUE_SIZE;
	idx_t MAIN_QUEUE_SIZE;
	idx_t GHOST_QUEUE_SIZE;

	//! Locked, if we're trying to forcefully evict a node.
	//! Only lets a single thread enter the phase.
	mutex purge_lock;
};

//! TODO: Use locking in places 
void S3FifoQueue::QueueInsert(shared_ptr<BlockHandle> handle) {
	switch (handle->GetQueueType()) {
	case S3FifoQueueType::NO_QUEUE:
		ProbationaryQueueInsert(S3FifoNode(weak_ptr<BlockHandle>(handle)));
		break;

	case S3FifoQueueType::PROBATIONARY_QUEUE:
	case S3FifoQueueType::MAIN_QUEUE:
	 	//! TODO: Cap this
		handle->IncrementAccesses();
		break;
	
	case S3FifoQueueType::GHOST_QUEUE:
		//! Move to main queue
		MainQueueInsert(S3FifoNode(weak_ptr<BlockHandle>(handle)));
	}
}

bool S3FifoQueue::Evict() {
	if (MainQueueEvict()) {
		return true;
	}
	if (GhostQueueEvict()) {
		return true;
	}
	return false;
}

void S3FifoQueue::ProbationaryQueueInsert(S3FifoNode &&node) {
	//! TODO: Evict node if full
	while (probationary_queue.size_approx() >= PROBATIONARY_QUEUE_SIZE) {
		ProbationaryQueueEvict();
	}

	auto handle_p = node.handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return;
	}
	
	// Reset access frequency
	handle_p->ResetAccesses();
	probationary_queue.enqueue(std::move(node));
}

void S3FifoQueue::MainQueueInsert(S3FifoNode &&node, uint8_t default_accesses) {
	//! TODO: Evict node if full
	while (main_queue.size_approx() >= MAIN_QUEUE_SIZE) {
		MainQueueEvict();
	}

	//! Get a reference to the underlying block pointer
	auto handle_p = node.handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return;
	}
	
	handle_p->SetQueueType(S3FifoQueueType::MAIN_QUEUE);

	// Reset access frequency
	if (default_accesses > 0) {
		handle_p->SetAccesses(default_accesses);
	} else {
		handle_p->ResetAccesses();
	}
	main_queue.enqueue(std::move(node));
}

void S3FifoQueue::GhostQueueInsert(S3FifoNode &&node) {
	//! TODO: Evict node if full
	while (ghost_queue.size_approx() >= GHOST_QUEUE_SIZE) {
		GhostQueueEvict();
	}

	//! Get a reference to the underlying block pointer
	auto handle_p = node.handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return;
	}
	
	handle_p->SetQueueType(S3FifoQueueType::GHOST_QUEUE);

	// Reset access frequency
	handle_p->ResetAccesses();
	ghost_queue.enqueue(std::move(node));
}

bool S3FifoQueue::ProbationaryQueueEvict() {
	S3FifoNode node;
	if (!probationary_queue.try_dequeue(node)) {
		//! TODO: Handle if try dequeue fails, try dequeue with lock?
		return false;
	}

	//! Get a reference to the underlying block pointer
	auto handle_p = node.handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return true;
	}

	handle_p->SetQueueType(S3FifoQueueType::PROBATIONARY_QUEUE);

	// Check the node access frequency
	auto accesses = handle_p->GetAccesses();
	if (accesses > 0) {
		// Move to main queue
		MainQueueInsert(std::move(node));
	} else {
		// Move to ghost queue
		GhostQueueInsert(std::move(node));
	}

	return true;
}

bool S3FifoQueue::MainQueueEvict() {
	//! TODO: What to do if loop loop loop
	for (int i = 0; i < MAIN_QUEUE_SIZE; i++) {
		S3FifoNode node;
		if (!main_queue.try_dequeue(node)) {
			//! TODO: Handle if try dequeue fails, try dequeue with lock?
			return false;
		}

		//! Get a reference to the underlying block pointer
		auto handle_p = node.handle.lock();
		if (!handle_p) {
			// BlockHandle has been destroyed
			return true;
		}

		//! TODO: Modify CanUnload
		//! Grab the mutex and check if we can free the block
		auto lock = handle_p->GetLock();
		
		if (handle_p->IsUnloaded()) {
			return true;
		}

		if (!handle_p->CanUnload()) {
			//! Lazy promotion
			MainQueueInsert(std::move(node), 1);
			continue;
		}

		//! Check the node access frequency
		auto accesses = handle_p->GetAccesses();
		if (accesses > 0) {
			//! Lazy promotion
			MainQueueInsert(std::move(node));
			continue;
		}

		//! TODO: Modify Unload if necessary
		//! Release the memory and mark the block as unloaded
		handle_p->Unload(lock);
		return true;
	}

	return false;
}

bool S3FifoQueue::GhostQueueEvict() {
	S3FifoNode node;
	if (!ghost_queue.try_dequeue(node)) {
		//! TODO: Handle if try dequeue fails, try dequeue with lock?
		return false;
	}

	//! Get a reference to the underlying block pointer
	auto handle_p = node.handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return true;
	}

	if (handle_p->GetQueueType() == S3FifoQueueType::MAIN_QUEUE) {
		return true;
	}

	//! TODO: Modify CanUnload
	//! Grab the mutex and check if we can free the block
	auto lock = handle_p->GetLock();
	if (handle_p->IsUnloaded()) {
		return true;
	}

	// D_ASSERT(handle_p->CanUnload());
	if (!handle_p->CanUnload()) {
		if (handle_p->GetQueueType() == S3FifoQueueType::GHOST_QUEUE) {
			handle_p->SetQueueType(S3FifoQueueType::NO_QUEUE);
		}
		//! Keep in buffer pool
		QueueInsert(handle_p);
		return true;
	}

	//! TODO: Modify Unload if necessary
	//! Release the memory and mark the block as unloaded
	handle_p->Unload(lock);

	return true;
}

BufferPool::BufferPool(idx_t maximum_memory, bool track_eviction_timestamps,
                       idx_t allocator_bulk_deallocation_flush_threshold)
    : maximum_memory(maximum_memory),
      allocator_bulk_deallocation_flush_threshold(allocator_bulk_deallocation_flush_threshold),
      track_eviction_timestamps(track_eviction_timestamps),
      temporary_memory_manager(make_uniq<TemporaryMemoryManager>()) {
	for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
		const auto type = static_cast<FileBufferType>(type_idx + 1);
		fifo_queues.push_back(make_uniq<S3FifoQueue>(type, maximum_memory));
	}
}
BufferPool::~BufferPool() {
}
 
void BufferPool::AddToQueue(shared_ptr<BlockHandle> &handle) {
	auto &queue = GetS3FifoQueueForBlockHandle(*handle);
	queue.QueueInsert(handle);
}

S3FifoQueue &BufferPool::GetS3FifoQueueForBlockHandle(const BlockHandle &handle) {
	const auto &handle_buffer_type = handle.GetBufferType();

	// Get offset into eviction queues for this FileBufferType
	for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
		const auto queue_buffer_type = static_cast<FileBufferType>(type_idx + 1);
		if (handle_buffer_type == queue_buffer_type) {
			return *fifo_queues[type_idx];
		}
	}

	D_ASSERT(false);
}

void BufferPool::UpdateUsedMemory(MemoryTag tag, int64_t size) {
	memory_usage.UpdateUsedMemory(tag, size);
}

idx_t BufferPool::GetUsedMemory() const {
	return memory_usage.GetUsedMemory(MemoryUsageCaches::FLUSH);
}

idx_t BufferPool::GetMaxMemory() const {
	return maximum_memory;
}

idx_t BufferPool::GetQueryMaxMemory() const {
	return GetMaxMemory();
}

TemporaryMemoryManager &BufferPool::GetTemporaryMemoryManager() {
	return *temporary_memory_manager;
}

BufferPool::EvictionResult BufferPool::EvictBlocks(MemoryTag tag, idx_t extra_memory, idx_t memory_limit,
                                                   unique_ptr<FileBuffer> *buffer) {
	for (auto &queue : fifo_queues) {
		//! TODO: Check code
		auto block_result = EvictBlocksInternal(*queue, tag, extra_memory, memory_limit, buffer);
		if (block_result.success || RefersToSameObject(*queue, *fifo_queues.back())) {
			return block_result; // Return upon success or upon last queue
		}
	}

	// This can never happen since we always return when i == 1. Exception to silence compiler warning
	throw InternalException("Exited BufferPool::EvictBlocksInternal without obtaining BufferPool::EvictionResult");
}

//! TODO: Do we need extra_memory == freed block exactly optimization
//! TODO: Should the logic be amended for individual file buffer types?
BufferPool::EvictionResult BufferPool::EvictBlocksInternal(S3FifoQueue &queue, MemoryTag tag, idx_t extra_memory,
                                                           idx_t memory_limit, unique_ptr<FileBuffer> *buffer) {
	TempBufferPoolReservation r(tag, *this, extra_memory);
	bool found = false;

	if (memory_usage.GetUsedMemory(MemoryUsageCaches::NO_FLUSH) <= memory_limit) {
		if (Allocator::SupportsFlush() && extra_memory > allocator_bulk_deallocation_flush_threshold) {
			Allocator::FlushAll();
		}
		return {true, std::move(r)};
	}

	while (true) {
		auto evicted = queue.Evict();

		if (memory_usage.GetUsedMemory(MemoryUsageCaches::NO_FLUSH) <= memory_limit) {
			found = true;
			break;
		}

		if (!evicted) {
			break;
		}
	}

	if (!found) {
		r.Resize(0);
	} else if (Allocator::SupportsFlush() && extra_memory > allocator_bulk_deallocation_flush_threshold) {
		Allocator::FlushAll();
	}

	return {found, std::move(r)};
}

void BufferPool::SetLimit(idx_t limit, const char *exception_postscript) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	//! TODO: Resize queues based on memory limit
	if (!EvictBlocks(MemoryTag::EXTENSION, 0, limit).success) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(MemoryTag::EXTENSION, 0, limit).success) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
}

void BufferPool::SetAllocatorBulkDeallocationFlushThreshold(idx_t threshold) {
	allocator_bulk_deallocation_flush_threshold = threshold;
}

idx_t BufferPool::GetAllocatorBulkDeallocationFlushThreshold() {
	return allocator_bulk_deallocation_flush_threshold;
}

BufferPool::MemoryUsage::MemoryUsage() {
	for (auto &v : memory_usage) {
		v = 0;
	}
	for (auto &cache : memory_usage_caches) {
		for (auto &v : cache) {
			v = 0;
		}
	}
}

void BufferPool::MemoryUsage::UpdateUsedMemory(MemoryTag tag, int64_t size) {
	auto tag_idx = (idx_t)tag;
	if ((idx_t)AbsValue(size) < MEMORY_USAGE_CACHE_THRESHOLD) {
		// update cache and update global counter when cache exceeds threshold
		// Get corresponding cache slot based on current CPU core index
		// Two threads may access the same cache simultaneously,
		// ensuring correctness through atomic operations
		auto cache_idx = (idx_t)TaskScheduler::GetEstimatedCPUId() % MEMORY_USAGE_CACHE_COUNT;
		auto &cache = memory_usage_caches[cache_idx];
		auto new_tag_size = cache[tag_idx].fetch_add(size, std::memory_order_relaxed) + size;
		if ((idx_t)AbsValue(new_tag_size) >= MEMORY_USAGE_CACHE_THRESHOLD) {
			// cached tag memory usage exceeds threshold
			auto tag_size = cache[tag_idx].exchange(0, std::memory_order_relaxed);
			memory_usage[tag_idx].fetch_add(tag_size, std::memory_order_relaxed);
		}
		auto new_total_size = cache[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size, std::memory_order_relaxed) + size;
		if ((idx_t)AbsValue(new_total_size) >= MEMORY_USAGE_CACHE_THRESHOLD) {
			// cached total memory usage exceeds threshold
			auto total_size = cache[TOTAL_MEMORY_USAGE_INDEX].exchange(0, std::memory_order_relaxed);
			memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(total_size, std::memory_order_relaxed);
		}
	} else {
		// update global counter
		memory_usage[tag_idx].fetch_add(size, std::memory_order_relaxed);
		memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size, std::memory_order_relaxed);
	}
}

} // namespace duckdb
