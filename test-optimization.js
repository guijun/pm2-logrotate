var fs = require('graceful-fs');
var zlib = require('zlib');
var { GeneratorBatchProcessor } = require('./utils/generator-batch-processor');

var TEST_FILE = '/tmp/test-log-optimization.log';
var OUTPUT_FILE = '/tmp/test-output.log';

function generateTestFile(sizeMB) {
  return new Promise(function(resolve, reject) {
    var writeStream = fs.createWriteStream(TEST_FILE);
    var chunk = Buffer.alloc(64 * 1024, 'x');
    var chunksNeeded = (sizeMB * 1024 * 1024) / chunk.length;
    
    var i = 0;
    function write() {
      var ok = true;
      do {
        i++;
        if (i === chunksNeeded) {
          writeStream.write(chunk, function() {
            writeStream.end(resolve);
          });
        } else {
          ok = writeStream.write(chunk);
        }
      } while (i < chunksNeeded && ok);
      if (i < chunksNeeded) {
        writeStream.once('drain', write);
      }
    }
    write();
  });
}

function testOriginalPipe() {
  return new Promise(function(resolve) {
    var startTime = Date.now();
    var writeCount = 0;
    
    var readStream = fs.createReadStream(TEST_FILE);
    var writeStream = fs.createWriteStream(OUTPUT_FILE + '.original');
    
    var originalWrite = writeStream.write.bind(writeStream);
    writeStream.write = function(chunk) {
      writeCount++;
      return originalWrite(chunk);
    };
    
    readStream.pipe(writeStream);
    
    writeStream.on('finish', function() {
      var duration = Date.now() - startTime;
      console.log('Original Pipe:');
      console.log('  Write count:', writeCount);
      console.log('  Duration:', duration + 'ms');
      resolve({ writeCount, duration });
    });
  });
}

function testOptimizedPipe() {
  return new Promise(function(resolve) {
    var startTime = Date.now();
    var writeCount = 0;
    
    var readStream = fs.createReadStream(TEST_FILE);
    var writeStream = fs.createWriteStream(OUTPUT_FILE + '.optimized');
    
    function waitDrain(stream) {
      return new Promise(function(resolve) {
        stream.once('drain', resolve);
      });
    }
    
    var batchProcessor = new GeneratorBatchProcessor({
      batchSize: 1024,
      flushIntervalMs: 1000,
      mode: 'custom',
      onFlush: async function(chunks) {
        if (chunks.length === 0) return;
        var buffer = Buffer.concat(chunks);
        writeCount++;
        if (!writeStream.write(buffer)) {
          await waitDrain(writeStream);
        }
      }
    });
    
    readStream.on('data', function(chunk) {
      batchProcessor.add(chunk);
    });
    
    readStream.on('end', async function() {
      await batchProcessor.flush();
      writeStream.end();
    });
    
    writeStream.on('finish', function() {
      var duration = Date.now() - startTime;
      console.log('Optimized Pipe:');
      console.log('  Write count:', writeCount);
      console.log('  Duration:', duration + 'ms');
      resolve({ writeCount, duration });
    });
  });
}

function testOptimizedPipeWithCompression() {
  return new Promise(function(resolve) {
    var startTime = Date.now();
    var writeCount = 0;
    
    var readStream = fs.createReadStream(TEST_FILE);
    var gzip = zlib.createGzip({ level: zlib.Z_BEST_COMPRESSION, memLevel: zlib.Z_BEST_COMPRESSION });
    var writeStream = fs.createWriteStream(OUTPUT_FILE + '.optimized.gz');
    
    function waitDrain(stream) {
      return new Promise(function(resolve) {
        stream.once('drain', resolve);
      });
    }
    
    var batchProcessor = new GeneratorBatchProcessor({
      batchSize: 4096,
      flushIntervalMs: 1000,
      mode: 'custom',
      onFlush: async function(chunks) {
        if (chunks.length === 0) return;
        var buffer = Buffer.concat(chunks);
        writeCount++;
        if (!gzip.write(buffer)) {
          await waitDrain(gzip);
        }
      }
    });
    
    readStream.on('data', function(chunk) {
      batchProcessor.add(chunk);
    });
    
    readStream.on('end', async function() {
      await batchProcessor.flush();
      gzip.end();
    });
    
    gzip.pipe(writeStream);
    
    writeStream.on('finish', function() {
      var duration = Date.now() - startTime;
      console.log('Optimized Pipe (with compression):');
      console.log('  Write count:', writeCount);
      console.log('  Duration:', duration + 'ms');
      resolve({ writeCount, duration });
    });
  });
}

async function runTests() {
  console.log('Generating test file (10MB)...');
  await generateTestFile(10);
  console.log('Test file generated.');
  
  console.log('\n--- Test 1: Original Pipe ---');
  var original = await testOriginalPipe();
  
  console.log('\n--- Test 2: Optimized Pipe ---');
  var optimized = await testOptimizedPipe();
  
  console.log('\n--- Test 3: Optimized Pipe with Compression ---');
  var optimizedCompressed = await testOptimizedPipeWithCompression();
  
  var reduction = ((original.writeCount - optimized.writeCount) / original.writeCount * 100).toFixed(2);
  console.log('\n--- Comparison ---');
  console.log('Original write count:', original.writeCount);
  console.log('Optimized write count:', optimized.writeCount);
  console.log('IO reduction:', reduction + '%');
  console.log('Original duration:', original.duration + 'ms');
  console.log('Optimized duration:', optimized.duration + 'ms');
  
  var originalChecksum = fs.readFileSync(OUTPUT_FILE + '.original').toString('hex');
  var optimizedChecksum = fs.readFileSync(OUTPUT_FILE + '.optimized').toString('hex');
  console.log('\nData integrity check (non-compressed):');
  console.log('  Original checksum:', originalChecksum.substring(0, 32) + '...');
  console.log('  Optimized checksum:', optimizedChecksum.substring(0, 32) + '...');
  console.log('  Match:', originalChecksum === optimizedChecksum ? 'YES' : 'NO');
  
  fs.unlinkSync(TEST_FILE);
  fs.unlinkSync(OUTPUT_FILE + '.original');
  fs.unlinkSync(OUTPUT_FILE + '.optimized');
  fs.unlinkSync(OUTPUT_FILE + '.optimized.gz');
  
  console.log('\nAll tests completed successfully!');
}

runTests().catch(function(err) {
  console.error('Test failed:', err);
});