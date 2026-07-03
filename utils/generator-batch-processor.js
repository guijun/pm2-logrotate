/**
 * GeneratorBatchProcessor - Batch processing utility for managing queued operations
 * 
 * Conversion Notes from TypeScript to JavaScript:
 * - Removed generic type parameters (<T>) since JS doesn't support static typing
 * - Removed interface definitions (GeneratorBatchProcessorConfig, FlushMode)
 * - Converted class properties to constructor assignments
 * - Kept all core logic intact: double-buffer pattern, drain mechanism, and flush modes
 * - Maintained the same public API for compatibility
 */

/**
 * Default configuration values for the batch processor
 */
const DEFAULT_CONFIG = {
  batchSize: 100,
  flushIntervalMs: 1000,
  nextMinIntervalMs: 100,
  autoStart: true,
};

/**
 * Internal logger for error/warning messages
 */
const internalLogger = {
  error: (msg, data) => {
    console.error('[GeneratorBatchProcessor]', msg, data);
  },
  warn: (msg, data) => {
    console.warn('[GeneratorBatchProcessor]', msg, data);
  },
};

/**
 * SliceWithReserve - A dynamic array implementation with capacity management
 * 
 * Key Features:
 * - Uses a fixed-size buffer with separate "filled" counter for efficient memory usage
 * - Automatically expands buffer when full (doubles capacity, minimum 100)
 * - Supports batch additions and swap operations for efficient buffer management
 * 
 * Design Rationale:
 * - The double-buffer pattern (writeBuffer + readBuffer) requires efficient swap operations
 * - Manual buffer management avoids overhead of frequent Array.prototype operations
 * - Capacity doubling ensures O(n) amortized time complexity for add operations
 */
class SliceWithReserve {
  /**
   * Create a new SliceWithReserve instance
   * @param {number} [capacity=100] - Initial buffer capacity
   */
  constructor(capacity = 100) {
    this.buffer = new Array(capacity);
    this.filled = 0;
  }

  /**
   * Get the number of items currently stored
   * @returns {number}
   */
  get length() {
    return this.filled;
  }

  /**
   * Get the current buffer capacity
   * @returns {number}
   */
  get capacity() {
    return this.buffer.length;
  }

  /**
   * Check if the buffer is empty
   * @returns {boolean}
   */
  isEmpty() {
    return this.filled === 0;
  }

  /**
   * Add a single item to the buffer
   * @param {*} item - The item to add
   */
  add(item) {
    if (this.filled >= this.buffer.length) {
      const newCapacity = Math.max(this.buffer.length * 2, 100);
      // Use slice() for cleaner array copying instead of manual for-loop
      this.buffer = this.buffer.slice(0, this.filled).concat(new Array(newCapacity - this.filled));
    }
    this.buffer[this.filled++] = item;
  }

  /**
   * Add multiple items to the buffer
   * @param {Array} items - Array of items to add
   */
  addBatch(items) {
    const needed = this.filled + items.length;
    if (needed > this.buffer.length) {
      const newCapacity = Math.max(needed, this.buffer.length * 2);
      // Use spread syntax for cleaner array concatenation
      this.buffer = [...this.buffer.slice(0, this.filled), ...new Array(newCapacity - this.filled)];
    }
    // Copy items using for-loop for performance with large batches
    for (let i = 0; i < items.length; i++) {
      this.buffer[this.filled + i] = items[i];
    }
    this.filled += items.length;
  }

  /**
   * Swap buffers with another SliceWithReserve instance
   * This is the core operation for the double-buffer pattern
   * @param {SliceWithReserve} other - The instance to swap with
   */
  swap(other) {
    const tempBuffer = this.buffer;
    const tempFilled = this.filled;
    this.buffer = other.buffer;
    this.filled = other.filled;
    other.buffer = tempBuffer;
    other.filled = tempFilled;
  }

  /**
   * Clear all items (resets filled counter, doesn't reallocate)
   */
  clear() {
    this.filled = 0;
  }

  /**
   * Get the underlying buffer array
   * Note: Returns the full buffer, not just the filled portion
   * @returns {Array}
   */
  getData() {
    return this.buffer;
  }

  /**
   * Set the filled counter directly
   * Used by the drain mechanism to track read position
   * @param {number} filled - New filled count
   */
  setFilled(filled) {
    this.filled = filled;
  }
}

/**
 * GeneratorBatchProcessor - Main class for batch processing
 * 
 * Flush Modes:
 * - 'chunk': Processes items in chunks of batchSize using onProcess callback
 * - 'custom': Processes all buffered items at once using onFlush callback
 * 
 * Double-Buffer Pattern:
 * - writeBuffer: Accepts new items via add()/addBatch()
 * - readBuffer: Items being processed by drainBuffer()
 * - swap(): Efficiently exchanges buffers when readBuffer is exhausted
 * 
 * Drain Mechanism:
 * - tryDrainBuffer(): Attempts to drain if not already draining
 * - drainBuffer(): Swaps buffers if needed and processes items
 * - executeFlush(): Processes a chunk or full batch based on mode
 */
class GeneratorBatchProcessor {
  /**
   * Create a new GeneratorBatchProcessor instance
   * @param {Object} config - Configuration options
   * @param {number} [config.batchSize=100] - Maximum items per batch
   * @param {number} [config.flushIntervalMs=1000] - Auto-flush interval (not currently used)
   * @param {boolean} [config.autoStart=true] - Start processor immediately
   * @param {Function} [config.onProcess] - Chunk processing callback (for 'chunk' mode)
   * @param {Function} [config.onFlush] - Batch processing callback (for 'custom' mode)
   * @param {Function} [config.onError] - Error handling callback
   */
  constructor(config) {
    this.batchSize = config.batchSize ?? DEFAULT_CONFIG.batchSize;
    this.flushIntervalMs = config.flushIntervalMs ?? DEFAULT_CONFIG.flushIntervalMs;
    this.nextMinIntervalMs = config.nextMinIntervalMs ?? DEFAULT_CONFIG.nextMinIntervalMs;
    this.writeBuffer = new SliceWithReserve(this.batchSize);
    this.readBuffer = new SliceWithReserve(this.batchSize);
    this.onError = config.onError ?? ((err) => internalLogger.error('Processing error', err));
    this.draining = false;
    this.readPosition = 0;
    this.stopped = false;
    this.drainTimer = null;

    // Determine mode based on provided callbacks
    if (config.onFlush) {
      this.mode = 'custom';
      this.onFlush = config.onFlush;
      this.onProcess = () => Promise.resolve();
    } else if (config.onProcess) {
      this.mode = 'chunk';
      this.onProcess = config.onProcess;
      this.onFlush = () => Promise.resolve();
    } else {
      throw new Error('GeneratorBatchProcessor: either onProcess or onFlush must be provided');
    }

    if (config.autoStart ?? DEFAULT_CONFIG.autoStart) {
      this.start();
    }
  }

  /**
   * Start the processor (currently a no-op, reserved for future use)
   */
  start() {}

  /**
   * Stop the processor and flush all remaining items
   * @returns {Promise<void>}
   */
  async stop() {
    await this.flushAll();
    this.stopped = true;
  }

  /**
   * Add a single item to the queue
   * Uses timer-based batching: items are collected and processed together
   * @param {*} item - The item to add
   */
  add(item) {
    if (this.stopped) {
      this.writeBuffer.add(item);
      return;
    }
    this.writeBuffer.add(item);
    // If buffer reaches batch size, process immediately
    if (this.writeBuffer.length >= this.batchSize) {
      this.tryDrainBuffer();
    } else {
      // Otherwise, schedule processing after a short delay
      // This allows multiple items to be collected into one batch
      this.scheduleDrain();
    }
  }

  /**
   * Add multiple items to the queue
   * @param {Array} items - Array of items to add
   */
  addBatch(items) {
    if (this.stopped) {
      this.writeBuffer.addBatch(items);
      return;
    }
    this.writeBuffer.addBatch(items);
    if (this.writeBuffer.length >= this.batchSize) {
      this.tryDrainBuffer();
    } else {
      this.scheduleDrain();
    }
  }

  /**
   * Schedule a drain operation with debouncing
   * Ensures multiple rapid add() calls are batched together
   */
  scheduleDrain() {
    if (this.drainTimer) return;
    // Use flushIntervalMs or default to 50ms for batching
    const delay = this.flushIntervalMs || 50;
    this.drainTimer = setTimeout(() => {
      this.drainTimer = null;
      this.tryDrainBuffer();
    }, delay);
  }

  /**
   * Attempt to drain the buffer if not already draining
   * @param {boolean} [doFlush=false] - If true, flush all items immediately
   * @returns {Promise<void>}
   */
  async tryDrainBuffer(doFlush) {
    if (this.draining) return;
    this.draining = true;
    try {
      if (doFlush) {
        // Flush mode: process until both buffers are empty
        while (!this.writeBuffer.isEmpty() || this.readPosition < this.readBuffer.length) {
          await this.drainBuffer();
        }
      } else {
        // Normal mode: process batches until buffer is empty
        // Use setImmediate to yield to event loop between batches
        let itemCount;
        do {
          itemCount = await this.drainBuffer();
          if (itemCount > 0 && (this.writeBuffer.isEmpty() && this.readPosition >= this.readBuffer.length)) {
            break;
          }
          // Yield to event loop to prevent blocking
          await new Promise(resolve => setImmediate(resolve));
        } while (itemCount > 0);
      }
    } finally {
      this.draining = false;
    }
    // Check if more items arrived during processing and schedule next drain
    if (!doFlush && (!this.writeBuffer.isEmpty() || this.readPosition < this.readBuffer.length)) {
      if (!this.drainTimer) {
        this.drainTimer = setTimeout(() => {
          this.drainTimer = null;
          this.tryDrainBuffer();
        }, this.nextMinIntervalMs);
      }
    }
  }

  /**
   * Drain a single batch from the buffer
   * Handles buffer swapping when readBuffer is exhausted
   * @returns {Promise<number>} - Number of items processed
   */
  async drainBuffer() {
    if (this.stopped) return 0;

    // Swap buffers if readBuffer is exhausted
    if (this.readPosition >= this.readBuffer.length) {
      this.readBuffer.clear();
      this.readBuffer.swap(this.writeBuffer);
      this.readPosition = 0;
    }

    const itemCount = this.readBuffer.length;
    if (itemCount === 0) return 0;

    try {
      this.readPosition = await this.executeFlush(this.readBuffer, this.readPosition);
    } catch (error) {
      try {
        internalLogger.error('Drain error', error);
      } catch (e) {
        console.error('[GeneratorBatchProcessor] Failed to log drain error:', e);
      }
    }

    return itemCount;
  }

  /**
   * Wait for current drain to complete, then flush all items
   * @returns {Promise<void>}
   */
  async flush() {
    while (this.draining) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    return this.tryDrainBuffer(true);
  }

  /**
   * Execute the flush operation for a portion of the buffer
   * @param {SliceWithReserve} buffer - The buffer to process
   * @param {number} readPosition - Current read position in the buffer
   * @returns {Promise<number>} - New read position after processing
   */
  async executeFlush(buffer, readPosition) {
    const items = buffer.getData();
    const itemCount = buffer.length;
    if (itemCount === 0 || this.stopped || readPosition >= itemCount) return readPosition;

    const start = readPosition;
    const end = Math.min(start + this.batchSize, itemCount);

    if (this.mode === 'custom') {
      // Custom mode: pass the batch directly to onFlush
      const batch = items.slice(start, end);
      await this.onFlush(batch);
    } else {
      // Chunk mode: process through onProcess callback
      const chunk = items.slice(start, end);
      await this.executeChunk(chunk);
    }

    return end;
  }

  /**
   * Execute a single chunk of items
   * @param {Array} chunk - The chunk of items to process
   * @returns {Promise<void>}
   */
  async executeChunk(chunk) {
    if (chunk.length === 0 || this.stopped) return;

    try {
      await this.onProcess(chunk);
    } catch (error) {
      const errorObj = error instanceof Error ? error : new Error(String(error));
      try {
        this.onError(errorObj, chunk);
      } catch (e) {
        console.error('[GeneratorBatchProcessor] Failed to call onError:', e);
      }
      throw errorObj;
    }
  }

  /**
   * Flush all remaining items (alias for flush())
   * @returns {Promise<void>}
   */
  flushAll() {
    return this.flush();
  }

  /**
   * Get the number of items waiting in the write buffer
   * @returns {number}
   */
  getBufferSize() {
    return this.writeBuffer.length;
  }

  /**
   * Get the configured batch size
   * @returns {number}
   */
  getBatchSize() {
    return this.batchSize;
  }

  /**
   * Check if the processor is currently draining
   * @returns {boolean}
   */
  isProcessing() {
    return this.draining;
  }

  /**
   * Get the current flush mode
   * @returns {'chunk' | 'custom'}
   */
  getMode() {
    return this.mode;
  }

  /**
   * Clear both buffers immediately
   */
  clear() {
    this.writeBuffer.clear();
    this.readBuffer.clear();
  }

  /**
   * Get a copy of the items in the write buffer
   * @returns {Array}
   */
  getBuffer() {
    const data = this.writeBuffer.getData();
    return data.slice(0, this.writeBuffer.length);
  }
}

module.exports = { GeneratorBatchProcessor };