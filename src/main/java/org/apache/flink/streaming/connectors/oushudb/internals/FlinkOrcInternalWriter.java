package org.apache.flink.streaming.connectors.oushudb.internals;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.connectors.oushudb.FlinkOushuDBErrorCode;
import org.apache.flink.streaming.connectors.oushudb.FlinkOushuDBException;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@PublicEvolving
public class FlinkOrcInternalWriter {
    private final Logger LOG = LoggerFactory.getLogger(FlinkOrcInternalWriter.class);

    private final Object producerClosingLock;
    private volatile boolean closed;
    private BulkWriter<RowData> writer;
    private Path tempDir;
    private FSDataOutputStream out;

    private final String transactionalId;
    private final OrcBulkWriterFactory<RowData> writerFactory;
    private final OushuDBTableInfo tableInfo;
    private final String temporary;
    private final FileSystem fs;
    /**
     * Number of unacknowledged records.
     */
    protected final AtomicLong pendingRecords = new AtomicLong();

    public FlinkOrcInternalWriter(String transactionalId,
                                  OrcBulkWriterFactory<RowData> writer,
                                  OushuDBTableInfo tableInfo,
                                  String temporary,
                                  Connection connection,
                                  FileSystem fs
    ) {
        this.transactionalId = transactionalId;
        this.writerFactory = writer;
        this.tableInfo = tableInfo;
        this.temporary = temporary;
        this.fs = fs;
        this.tableInfo.setConnection(connection);

        this.producerClosingLock = new Object();
        this.closed = false;
    }

    public void beginTransactions() throws Exception {
        synchronized (producerClosingLock) {
            ensureNotClosed();
            tempDir = new Path(temporary, transactionalId);
            if (!fs.mkdirs(tempDir)) {
                throw new FlinkOushuDBException(FlinkOushuDBErrorCode.EXTERNAL_ERROR,
                    "failed to create temp folder " + tempDir.toString());
            }
            LOG.info("Created temporary folder {}", tempDir.toString());
            Path outFile = new Path(tempDir, UUID.randomUUID().toString());
            out = fs.create(outFile, FileSystem.WriteMode.OVERWRITE);
            writer = writerFactory.create(out);
        }
    }

    public void commitTransaction() throws RuntimeException {
        synchronized (producerClosingLock) {
            try {
                long records = pendingRecords.getAndSet(0);
                if (records > 0) {
                    commitToOushuDB();
                    LOG.info("{} records has been committed.", records);
                    LOG.info("Deleted temporary folder {} when abort transaction", tempDir);
                    deleteTemp();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }


    public void abortTransaction() throws RuntimeException {
        synchronized (producerClosingLock) {
            ensureNotClosed();
            try {
                LOG.info("Deleted temporary folder {} when abort transaction", tempDir);
                deleteTemp();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void addElement(RowData element) throws IOException {
        this.pendingRecords.incrementAndGet();
        this.writer.addElement(element);
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }
        synchronized (producerClosingLock) {
            LOG.info("Deleted temporary folder {} when close", tempDir);
            deleteTemp();
            closed = true;
        }
    }

    private void deleteTemp() throws IOException {
        if (tempDir != null) {
            fs.delete(tempDir, true);
            tempDir = null;
        }
    }

    public void flush() throws IOException {
        synchronized (producerClosingLock) {
            ensureNotClosed();
            writer.finish();
            out.flush(); // flush data to hdfs
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException(
                String.format(
                    "The writer %s has already been closed",
                    System.identityHashCode(this)));
        }
    }

    private void commitToOushuDB() throws Exception {
        if (this.tableInfo.exists()) {
            if (!this.tableInfo.checkCompatibility()) {
                throw new FlinkOushuDBException(FlinkOushuDBErrorCode.EXTERNAL_ERROR, "Insert data is incompatible with table types.");
            }
        } else {
            this.tableInfo.createTable();
        }
        this.tableInfo.insert(this.tempDir.toString());
    }

    public String getTransactionalId() {
        return transactionalId;
    }
}
