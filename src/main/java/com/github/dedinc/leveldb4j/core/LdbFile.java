package com.github.dedinc.leveldb4j.core;

import com.github.dedinc.leveldb4j.compression.SnappyDecompressor;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A LevelDB table (.ldb or .sst) file.
 * See: https://github.com/google/leveldb/blob/master/doc/table_format.md
 */
public class LdbFile implements Closeable, Iterable<Record> {
    private static final int BLOCK_TRAILER_SIZE = 5;
    private static final int FOOTER_SIZE = 48;
    private static final long MAGIC = 0xdb4775248b80fb57L;

    private final Path path;
    private final int fileNo;
    private final RandomAccessFile file;
    private final BlockHandle metaIndexHandle;
    private final BlockHandle indexHandle;
    private final List<IndexEntry> index;

    /**
     * Index entry containing the earliest key and block handle.
     */
    private static class IndexEntry {
        final byte[] key;
        final BlockHandle handle;

        IndexEntry(byte[] key, BlockHandle handle) {
            this.key = key;
            this.handle = handle;
        }
    }

    public LdbFile(Path path) throws IOException {
        this.path = path;

        // Extract file number from filename (hex format)
        String filename = path.getFileName().toString();
        String nameWithoutExt = filename.substring(0, filename.lastIndexOf('.'));
        this.fileNo = Integer.parseInt(nameWithoutExt, 16);

        this.file = new RandomAccessFile(path.toFile(), "r");

        // Read footer
        file.seek(file.length() - FOOTER_SIZE);
        byte[] footerData = new byte[FOOTER_SIZE];
        file.readFully(footerData);

        ByteArrayInputStream footerStream = new ByteArrayInputStream(footerData);
        this.metaIndexHandle = BlockHandle.fromStream(footerStream);
        this.indexHandle = BlockHandle.fromStream(footerStream);

        // Read and verify magic number
        file.seek(file.length() - 8);
        byte[] magicBytes = new byte[8];
        file.readFully(magicBytes);
        long magic = ByteBuffer.wrap(magicBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        if (magic != MAGIC) {
            throw new IOException(String.format("Invalid magic number in %s (expected 0x%x, got 0x%x)",
                    path, MAGIC, magic));
        }

        // Read index
        this.index = readIndex();
    }

    /**
     * Reads a block from the file.
     */
    private Block readBlock(BlockHandle handle) throws IOException {
        file.seek(handle.getOffset());
        byte[] rawBlock = new byte[(int) handle.getLength()];
        file.readFully(rawBlock);

        byte[] trailer = new byte[BLOCK_TRAILER_SIZE];
        file.readFully(trailer);

        // Check compression type (first byte of trailer)
        boolean isCompressed = trailer[0] != 0;

        if (isCompressed) {
            try {
                rawBlock = SnappyDecompressor.decompress(new ByteArrayInputStream(rawBlock));
            } catch (Exception e) {
                //ignore decompression when failed
            }
        }

        return new Block(rawBlock, isCompressed, this, handle.getOffset());
    }

    /**
     * Reads the index block.
     */
    private List<IndexEntry> readIndex() throws IOException {
        Block indexBlock = readBlock(indexHandle);
        List<IndexEntry> indexEntries = new ArrayList<>();

        for (RawBlockEntry entry : indexBlock) {
            BlockHandle handle = BlockHandle.fromBytes(entry.getValue());
            indexEntries.add(new IndexEntry(entry.getKey(), handle));
        }

        return indexEntries;
    }

    @Override
    public Iterator<Record> iterator() {
        return new LdbFileIterator();
    }

    /**
     * Iterator for records in this LDB file.
     */
    private class LdbFileIterator implements Iterator<Record> {
        private int indexPosition = 0;
        private Iterator<RawBlockEntry> currentBlockIterator;
        private Block currentBlock;

        LdbFileIterator() {
            advance();
        }

        private void advance() {
            // If we have a current block iterator with more entries, we're done
            if (currentBlockIterator != null && currentBlockIterator.hasNext()) {
                return;
            }

            // Move to next block
            if (indexPosition < index.size()) {
                try {
                    IndexEntry entry = index.get(indexPosition++);
                    currentBlock = readBlock(entry.handle);
                    currentBlockIterator = currentBlock.iterator();
                } catch (IOException e) {
                    currentBlockIterator = null;
                }
            } else {
                currentBlockIterator = null;
            }
        }

        @Override
        public boolean hasNext() {
            return currentBlockIterator != null && currentBlockIterator.hasNext();
        }

        @Override
        public Record next() {
            RawBlockEntry entry = currentBlockIterator.next();
            long offset = currentBlock.wasCompressed() ?
                    currentBlock.getOffset() :
                    currentBlock.getOffset() + entry.getBlockOffset();

            Record record = Record.ldbRecord(
                    entry.getKey(),
                    entry.getValue(),
                    path,
                    offset,
                    currentBlock.wasCompressed()
            );

            advance();
            return record;
        }
    }

    public Path getPath() {
        return path;
    }

    public int getFileNo() {
        return fileNo;
    }

    @Override
    public void close() throws IOException {
        if (file != null) {
            file.close();
        }
    }
}

