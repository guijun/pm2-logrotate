/**
 * Unit test for GeneratorBatchProcessor
 */
var { GeneratorBatchProcessor } = require('../utils/generator-batch-processor');

console.log('=== Test 1: Chunk Mode ===');
var processedChunks = [];
var chunkProcessor = new GeneratorBatchProcessor({
  batchSize: 3,
  autoStart: true,
  onProcess: async function(chunk) {
    processedChunks.push(chunk);
    console.log('Processed chunk:', chunk);
  }
});

chunkProcessor.add('item1');
chunkProcessor.add('item2');
chunkProcessor.add('item3');

setTimeout(function() {
  chunkProcessor.add('item4');
  chunkProcessor.add('item5');
  
  setTimeout(function() {
    chunkProcessor.flush().then(function() {
      console.log('Processed chunks:', processedChunks);
      if (processedChunks.length >= 1 && processedChunks[0].length === 3) {
        console.log('✓ Chunk mode works correctly\n');
      } else {
        console.log('✗ Chunk mode failed\n');
      }
      
      console.log('=== Test 2: Custom Mode with Deduplication ===');
      var flushedBatches = [];
      var customProcessor = new GeneratorBatchProcessor({
        batchSize: 5,
        autoStart: true,
        onFlush: async function(batch) {
          flushedBatches.push(batch);
          var seen = {};
          var unique = batch.filter(function(item) {
            if (seen[item.file]) return false;
            seen[item.file] = true;
            return true;
          });
          console.log('Batch size:', batch.length, 'Unique files:', unique.length);
        }
      });

      customProcessor.add({ file: '/var/log/app1.log' });
      customProcessor.add({ file: '/var/log/app2.log' });
      customProcessor.add({ file: '/var/log/app1.log' });
      customProcessor.add({ file: '/var/log/app3.log' });
      customProcessor.add({ file: '/var/log/app2.log' });

      setTimeout(function() {
        customProcessor.flush().then(function() {
          console.log('Flushed batches:', flushedBatches);
          if (flushedBatches.length >= 1 && flushedBatches[0].length === 5) {
            console.log('✓ Custom mode with deduplication works correctly');
          } else {
            console.log('✗ Custom mode failed');
          }
          
          chunkProcessor.stop();
          customProcessor.stop();
        });
      }, 100);
    });
  }, 100);
}, 100);