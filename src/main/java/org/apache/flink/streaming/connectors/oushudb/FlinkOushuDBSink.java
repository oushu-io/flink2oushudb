package org.apache.flink.streaming.connectors.oushudb;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.vector.Vectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.oushudb.internals.FlinkOrcInternalWriter;
import org.apache.flink.streaming.connectors.oushudb.internals.OushuDBTableInfo;
import org.apache.flink.streaming.connectors.oushudb.internals.TransactionalIdsGenerator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Flink Sink to data into OushuDB */
public class FlinkOushuDBSink
        extends TwoPhaseCommitSinkFunction<
                RowData,
                FlinkOushuDBSink.OushuDBTransactionState,
                FlinkOushuDBSink.OushuDBTransactionContext> {

    public static final String OUSHUDB_HOSTS = "oushudb.hosts";
    public static final String OUSHUDB_PORT = "oushudb.port";
    public static final String OUSHUDB_DATABASE = "oushudb.database";
    public static final String OUSHUDB_USER = "oushudb.user";
    public static final String OUSHUDB_PASSWORD = "oushudb.password";
    public static final String OUSHUDB_TEMP_PATH = "oushudb.temp.path";
    public static final int SAFE_SCALE_DOWN_FACTOR = 5;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkOushuDBSink.class);

    private static final long serialVersionUID = -1974448328476882174L;

    /** Properties for OushuDB */
    protected final Properties oushuDBConfig;

    protected final Vectorizer<RowData> vectorizer;

    protected final OushuDBTableInfo tableInfo;

    /** OushuDB SQL connection */
    protected Connection connection;

    /** ORC writer factory */
    private OrcBulkWriterFactory<RowData> writerFactory;

    private String temporary;

    /** Errors encountered in the async producer are stored here. */
    @Nullable protected transient volatile Exception asyncException;
    /** Pool of available transactional ids. */
    private final BlockingDeque<String> availableTransactionalIds = new LinkedBlockingDeque<>();

    public static final int DEFAULT_ORC_WRITER_POOL_SIZE = 5;

    /** HDFS file system */
    private FileSystem fs;

    private static final ListStateDescriptor<FlinkOushuDBSink.NextTransactionalIdHint>
            NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR =
                    new ListStateDescriptor<>(
                            "next-transactional-id-hint", new NextTransactionalIdHintSerializer());

    /** State for nextTransactionalIdHint. */
    private transient ListState<FlinkOushuDBSink.NextTransactionalIdHint>
            nextTransactionalIdHintState;

    /** Generator for Transactional IDs. */
    private transient TransactionalIdsGenerator transactionalIdsGenerator;

    /** Hint for picking next transactional id. */
    private transient NextTransactionalIdHint nextTransactionalIdHint;

    private int poolSize;

    /**
     * Creates a FlinkOushuDBSink for a given tableInfo.
     *
     * @param tableInfo OushuDB table information
     * @param oushuDBConfig OushuDB connection information
     */
    public FlinkOushuDBSink(OushuDBTableInfo tableInfo, Properties oushuDBConfig) {
        this(tableInfo, oushuDBConfig, DEFAULT_ORC_WRITER_POOL_SIZE);
    }

    /**
     * Creates a FlinkOushuDBSink for a given tableInfo.
     *
     * @param tableInfo OushuDB table information
     * @param oushuDBConfig OushuDB connection information
     */
    public FlinkOushuDBSink(OushuDBTableInfo tableInfo, Properties oushuDBConfig, int poolSize) {
        super(
                new FlinkOushuDBSink.TransactionStateSerializer(),
                new FlinkOushuDBSink.ContextStateSerializer());

        this.tableInfo = checkNotNull(tableInfo, "tableInfo is null");
        LogicalType[] types = tableInfo.getFieldsType();
        TypeDescription typeDescription =
                OrcSplitReaderUtil.logicalTypeToOrcType(
                        RowType.of(tableInfo.getFieldsType(), tableInfo.getFieldsName()));
        this.oushuDBConfig = checkNotNull(oushuDBConfig, "oushuDBConfig is null");
        this.vectorizer = new RowDataVectorizer(typeDescription.toString(), types);
        this.writerFactory = new OrcBulkWriterFactory<>(this.vectorizer);
        this.temporary = getConfig(OUSHUDB_TEMP_PATH);
        this.poolSize = poolSize;
    }

    private String getConfig(String key) throws IllegalArgumentException {
        String val = this.oushuDBConfig.getProperty(key);
        if (isNullOrEmpty(val)) {
            throw new IllegalArgumentException(
                    key + " must be supplied in the oushudb config properties.");
        }
        return val;
    }

    public static boolean isNullOrEmpty(@Nullable String string) {
        return string == null || string.length() == 0;
    }

    private void buildSQLConn() throws Exception {
        if (this.connection != null) return;

        String hostStr = getConfig(OUSHUDB_HOSTS);
        String port = getConfig(OUSHUDB_PORT);
        String database = getConfig(OUSHUDB_DATABASE);
        String user = getConfig(OUSHUDB_USER);
        String password = getConfig(OUSHUDB_PASSWORD);
        String[] hosts = hostStr.split(",");

        int attemptCount = 1;
        int i = 0;
        while (attemptCount <= hosts.length) {
            try {
                attemptCount++;
                Class.forName("org.postgresql.Driver");
                String url = "jdbc:postgresql://" + hosts[i] + ":" + port + "/" + database;
                i = (i + 1) % hosts.length;
                connection = DriverManager.getConnection(url, user, password);
                connection.prepareStatement("select 1").execute();
                return;
            } catch (Exception e) {
                LOG.warn("connect to oushudb failed:", e);
            }
        }
        throw new SQLException("connect to db failed with all oushudb hosts.");
    }

    private FlinkOrcInternalWriter createTransactionalWriter() throws Exception {
        String transactionalId = availableTransactionalIds.poll();
        if (transactionalId == null) {
            throw new FlinkOushuDBException(
                    FlinkOushuDBErrorCode.WRITER_POOL_EMPTY,
                    "Too many ongoing snapshots. Increase kafka producers pool size or decrease number of concurrent checkpoints.");
        }

        return new FlinkOrcInternalWriter(
                transactionalId, writerFactory, tableInfo, temporary, connection, fs);
    }

    private void recycleTransactionalWriter(FlinkOrcInternalWriter writer) {
        availableTransactionalIds.add(writer.getTransactionalId());
        try {
            writer.flush();
            LOG.info("recycleTransactionalWriter close");
            writer.close();
        } catch (IOException e) {
            asyncException = e;
        }
    }

    private void checkErroneous() throws FlinkOushuDBException {
        Exception e = asyncException;
        if (e != null) {
            // prevent double throwing
            asyncException = null;
            throw new FlinkOushuDBException(
                    FlinkOushuDBErrorCode.EXTERNAL_ERROR,
                    "Failed to write data to orc file: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    protected void invoke(OushuDBTransactionState transaction, RowData value, Context context)
            throws Exception {
        checkErroneous();
        transaction.writer.addElement(value);
    }

    @Override
    public void close() throws Exception {
        try {
            final OushuDBTransactionState currentTransaction = currentTransaction();
            if (currentTransaction != null) {
                flush(currentTransaction);
                currentTransaction.writer.flush();
                currentTransaction.writer.close();
            }
        } catch (Exception e) {
            asyncException = ExceptionUtils.firstOrSuppressed(e, asyncException);
        } finally {
            if (currentTransaction() != null) {
                try {
                    currentTransaction().writer.close();
                } catch (Throwable t) {
                    LOG.warn("Error closing writer.", t);
                }
            }
            // Make sure all the producers for pending transactions are closed.
            pendingTransactions()
                    .forEach(
                            transaction -> {
                                try {
                                    transaction.getValue().writer.close();
                                } catch (Throwable t) {
                                    LOG.warn("Error closing writer.", t);
                                }
                            });
            // make sure we propagate pending errors
            checkErroneous();
        }
        super.close();
    }

    @Override
    protected OushuDBTransactionState beginTransaction() throws Exception {
        FlinkOrcInternalWriter writer = createTransactionalWriter();
        writer.beginTransactions();
        return new OushuDBTransactionState(writer.getTransactionalId(), writer);
    }

    @Override
    protected void preCommit(OushuDBTransactionState transaction) throws Exception {
        flush(transaction);
        checkErroneous();
    }

    @Override
    protected void commit(OushuDBTransactionState transaction) {
        try {
            transaction.writer.commitTransaction();
        } finally {
            recycleTransactionalWriter(transaction.getWriter());
        }
    }

    @Override
    protected void recoverAndCommit(OushuDBTransactionState transaction) {
        FlinkOrcInternalWriter writer = null;
        try {
            writer =
                    new FlinkOrcInternalWriter(
                            transaction.transactionalId,
                            writerFactory,
                            tableInfo,
                            temporary,
                            connection,
                            fs);
            writer.commitTransaction();
        } catch (Exception ex) {
            LOG.warn(
                    "Encountered error {} while recovering transaction {}. "
                            + "Presumably this transaction has been already committed before",
                    ex,
                    transaction);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    LOG.warn("Encountered error {} while closing orc writer.", e.getMessage(), e);
                }
            }
        }
    }

    @Override
    protected void abort(OushuDBTransactionState transaction) {
        transaction.writer.abortTransaction();
        recycleTransactionalWriter(transaction.getWriter());
    }

    @Override
    protected void recoverAndAbort(OushuDBTransactionState transaction) {
        FlinkOrcInternalWriter writer = null;
        try {
            writer =
                    new FlinkOrcInternalWriter(
                            transaction.transactionalId,
                            writerFactory,
                            tableInfo,
                            temporary,
                            connection,
                            fs);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e) {
                    LOG.warn("Encountered error {} while closing orc writer.", e.getMessage(), e);
                }
            }
        }
    }

    private void flush(OushuDBTransactionState transaction) throws Exception {
        if (transaction.writer != null) {
            transaction.writer.flush();
        }

        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkErroneous();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        super.snapshotState(context);

        nextTransactionalIdHintState.clear();
        if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
            checkState(
                    nextTransactionalIdHint != null,
                    "nextTransactionalIdHint must be set for EXACTLY_ONCE");
            long nextFreeTransactionalId = nextTransactionalIdHint.nextFreeTransactionalId;

            // If we scaled up, some (unknown) subtask must have created new transactional ids from
            // scratch. In that
            // case we adjust nextFreeTransactionalId by the range of transactionalIds that could be
            // used for this
            // scaling up.
            if (getRuntimeContext().getNumberOfParallelSubtasks()
                    > nextTransactionalIdHint.lastParallelism) {
                nextFreeTransactionalId += getRuntimeContext().getNumberOfParallelSubtasks();
            }

            nextTransactionalIdHintState.add(
                    new NextTransactionalIdHint(
                            getRuntimeContext().getNumberOfParallelSubtasks(),
                            nextFreeTransactionalId));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        buildSQLConn();
        if (fs == null) {
            Path tempPath = new Path(temporary);
            fs = FileSystem.get(tempPath.toUri());
            if (!fs.exists(tempPath)) {
                throw new NoSuchFileException(
                        String.format("temp path [%s] not exists.", tempPath.toString()));
            }
        }

        nextTransactionalIdHintState =
                context.getOperatorStateStore()
                        .getUnionListState(NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR);

        transactionalIdsGenerator =
                new TransactionalIdsGenerator(
                        ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID(),
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        poolSize,
                        SAFE_SCALE_DOWN_FACTOR);
        ArrayList<FlinkOushuDBSink.NextTransactionalIdHint> transactionalIdHints =
                Lists.newArrayList(nextTransactionalIdHintState.get());
        if (transactionalIdHints.size() > 1) {
            throw new IllegalStateException(
                    "There should be at most one next transactional id hint written by the first subtask");
        } else if (transactionalIdHints.size() == 0) {
            nextTransactionalIdHint = new NextTransactionalIdHint(0, 0);

            // this means that this is either:
            // (1) the first execution of this application
            // (2) previous execution has failed before first checkpoint completed
            //
            // in case of (2) we have to abort all previous transactions
            abortTransactions(transactionalIdsGenerator.generateIdsToAbort());
        } else {
            nextTransactionalIdHint = transactionalIdHints.get(0);
        }
        super.initializeState(context);
    }

    @Override
    protected Optional<OushuDBTransactionContext> initializeUserContext() {
        Set<String> transactionalIds = generateNewTransactionalIds();
        resetAvailableTransactionalIdsPool(transactionalIds);
        return Optional.of(new OushuDBTransactionContext(transactionalIds));
    }

    @Override
    protected void finishRecoveringContext(
            Collection<OushuDBTransactionState> handledTransactions) {
        cleanUpUserContext(handledTransactions);
        resetAvailableTransactionalIdsPool(getUserContext().get().transactionalIds);
        LOG.info("Recovered transactionalIds {}", getUserContext().get().transactionalIds);
    }

    /**
     * After initialization make sure that all previous transactions from the current user context
     * have been completed.
     *
     * @param handledTransactions transactions which were already committed or aborted and do not
     *     need further handling
     */
    private void cleanUpUserContext(Collection<OushuDBTransactionState> handledTransactions) {
        if (!getUserContext().isPresent()) {
            return;
        }
        HashSet<String> abortTransactions = new HashSet<>(getUserContext().get().transactionalIds);
        handledTransactions.forEach(
                kafkaTransactionState ->
                        abortTransactions.remove(kafkaTransactionState.transactionalId));
        abortTransactions(abortTransactions);
    }

    private Set<String> generateNewTransactionalIds() {
        checkState(
                nextTransactionalIdHint != null,
                "nextTransactionalIdHint must be present for EXACTLY_ONCE");

        Set<String> transactionalIds =
                transactionalIdsGenerator.generateIdsToUse(
                        nextTransactionalIdHint.nextFreeTransactionalId);
        LOG.info("Generated new transactionalIds {}", transactionalIds);
        return transactionalIds;
    }

    private void resetAvailableTransactionalIdsPool(Collection<String> transactionalIds) {
        availableTransactionalIds.clear();
        availableTransactionalIds.addAll(transactionalIds);
    }

    private void abortTransactions(final Set<String> transactionalIds) {
        transactionalIds
                .parallelStream()
                .forEach(
                        transactionalId -> {
                            Path temp = new Path(temporary, transactionalId);
                            try {
                                fs.delete(temp, true);
                            } catch (IOException e) {
                                LOG.warn("failed to delete temporary folder " + temp.toString());
                            }
                        });
    }

    public static class OushuDBTransactionState {

        private final transient FlinkOrcInternalWriter writer;

        @Nullable final String transactionalId;

        @VisibleForTesting
        public OushuDBTransactionState(
                @Nullable String transactionalId, FlinkOrcInternalWriter writer) {
            this.transactionalId = transactionalId;
            this.writer = writer;
        }

        public FlinkOrcInternalWriter getWriter() {
            return writer;
        }

        @Override
        public String toString() {
            return String.format(
                    "%s [transactionalId=%s]", this.getClass().getSimpleName(), transactionalId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OushuDBTransactionState that = (OushuDBTransactionState) o;
            return Objects.equals(transactionalId, that.transactionalId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionalId);
        }
    }

    public static class OushuDBTransactionContext {
        final Set<String> transactionalIds;

        @VisibleForTesting
        public OushuDBTransactionContext(Set<String> transactionalIds) {
            checkNotNull(transactionalIds);
            this.transactionalIds = transactionalIds;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OushuDBTransactionContext that = (OushuDBTransactionContext) o;
            return transactionalIds.equals(that.transactionalIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionalIds);
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link
     * OushuDBTransactionState}.
     */
    @VisibleForTesting
    @Internal
    public static class TransactionStateSerializer
            extends TypeSerializerSingleton<OushuDBTransactionState> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public OushuDBTransactionState createInstance() {
            return null;
        }

        @Override
        public OushuDBTransactionState copy(OushuDBTransactionState from) {
            return from;
        }

        @Override
        public OushuDBTransactionState copy(
                OushuDBTransactionState from, OushuDBTransactionState reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(OushuDBTransactionState record, DataOutputView target)
                throws IOException {
            if (record.transactionalId == null) {
                target.writeBoolean(false);
            } else {
                target.writeBoolean(true);
                target.writeUTF(record.transactionalId);
            }
        }

        @Override
        public OushuDBTransactionState deserialize(DataInputView source) throws IOException {
            String transactionalId = null;
            if (source.readBoolean()) {
                transactionalId = source.readUTF();
            }
            return new OushuDBTransactionState(transactionalId, null);
        }

        @Override
        public OushuDBTransactionState deserialize(
                OushuDBTransactionState reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            boolean hasTransactionalId = source.readBoolean();
            target.writeBoolean(hasTransactionalId);
            if (hasTransactionalId) {
                target.writeUTF(source.readUTF());
            }
            target.writeLong(source.readLong());
            target.writeShort(source.readShort());
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<OushuDBTransactionState> snapshotConfiguration() {
            return new TransactionStateSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class TransactionStateSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<OushuDBTransactionState> {

            public TransactionStateSerializerSnapshot() {
                super(TransactionStateSerializer::new);
            }
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link
     * OushuDBTransactionContext}.
     */
    @VisibleForTesting
    @Internal
    public static class ContextStateSerializer
            extends TypeSerializerSingleton<OushuDBTransactionContext> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public OushuDBTransactionContext createInstance() {
            return null;
        }

        @Override
        public OushuDBTransactionContext copy(OushuDBTransactionContext from) {
            return from;
        }

        @Override
        public OushuDBTransactionContext copy(
                OushuDBTransactionContext from, OushuDBTransactionContext reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(OushuDBTransactionContext record, DataOutputView target)
                throws IOException {
            int numIds = record.transactionalIds.size();
            target.writeInt(numIds);
            for (String id : record.transactionalIds) {
                target.writeUTF(id);
            }
        }

        @Override
        public OushuDBTransactionContext deserialize(DataInputView source) throws IOException {
            int numIds = source.readInt();
            Set<String> ids = new HashSet<>(numIds);
            for (int i = 0; i < numIds; i++) {
                ids.add(source.readUTF());
            }
            return new OushuDBTransactionContext(ids);
        }

        @Override
        public OushuDBTransactionContext deserialize(
                OushuDBTransactionContext reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            int numIds = source.readInt();
            target.writeInt(numIds);
            for (int i = 0; i < numIds; i++) {
                target.writeUTF(source.readUTF());
            }
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<OushuDBTransactionContext> snapshotConfiguration() {
            return new ContextStateSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class ContextStateSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<OushuDBTransactionContext> {

            public ContextStateSerializerSnapshot() {
                super(ContextStateSerializer::new);
            }
        }
    }

    /** Keep information required to deduce next safe to use transactional id. */
    public static class NextTransactionalIdHint {
        public int lastParallelism = 0;
        public long nextFreeTransactionalId = 0;

        public NextTransactionalIdHint() {
            this(0, 0);
        }

        public NextTransactionalIdHint(int parallelism, long nextFreeTransactionalId) {
            this.lastParallelism = parallelism;
            this.nextFreeTransactionalId = nextFreeTransactionalId;
        }

        @Override
        public String toString() {
            return "NextTransactionalIdHint["
                    + "lastParallelism="
                    + lastParallelism
                    + ", nextFreeTransactionalId="
                    + nextFreeTransactionalId
                    + ']';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NextTransactionalIdHint that = (NextTransactionalIdHint) o;

            if (lastParallelism != that.lastParallelism) {
                return false;
            }
            return nextFreeTransactionalId == that.nextFreeTransactionalId;
        }

        @Override
        public int hashCode() {
            int result = lastParallelism;
            result =
                    31 * result
                            + (int) (nextFreeTransactionalId ^ (nextFreeTransactionalId >>> 32));
            return result;
        }
    }

    /**
     * {@link org.apache.flink.api.common.typeutils.TypeSerializer} for {@link
     * FlinkOushuDBSink.NextTransactionalIdHint}.
     */
    @VisibleForTesting
    @Internal
    public static class NextTransactionalIdHintSerializer
            extends TypeSerializerSingleton<NextTransactionalIdHint> {

        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public NextTransactionalIdHint createInstance() {
            return new NextTransactionalIdHint();
        }

        @Override
        public NextTransactionalIdHint copy(NextTransactionalIdHint from) {
            return from;
        }

        @Override
        public NextTransactionalIdHint copy(
                NextTransactionalIdHint from, NextTransactionalIdHint reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return Long.BYTES + Integer.BYTES;
        }

        @Override
        public void serialize(NextTransactionalIdHint record, DataOutputView target)
                throws IOException {
            target.writeLong(record.nextFreeTransactionalId);
            target.writeInt(record.lastParallelism);
        }

        @Override
        public NextTransactionalIdHint deserialize(DataInputView source) throws IOException {
            long nextFreeTransactionalId = source.readLong();
            int lastParallelism = source.readInt();
            return new NextTransactionalIdHint(lastParallelism, nextFreeTransactionalId);
        }

        @Override
        public NextTransactionalIdHint deserialize(
                NextTransactionalIdHint reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        }

        @Override
        public TypeSerializerSnapshot<NextTransactionalIdHint> snapshotConfiguration() {
            return new NextTransactionalIdHintSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class NextTransactionalIdHintSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<NextTransactionalIdHint> {

            public NextTransactionalIdHintSerializerSnapshot() {
                super(NextTransactionalIdHintSerializer::new);
            }
        }
    }
}
