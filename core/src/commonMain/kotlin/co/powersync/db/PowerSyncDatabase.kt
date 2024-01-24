package co.powersync.db

import app.cash.sqldelight.ExecutableQuery
import app.cash.sqldelight.Query
import app.cash.sqldelight.SuspendingTransactionWithReturn
import app.cash.sqldelight.SuspendingTransactionWithoutReturn
import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.async.coroutines.awaitAsOneOrNull
import app.cash.sqldelight.async.coroutines.await
import app.cash.sqldelight.coroutines.asFlow
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlPreparedStatement
import co.powersync.Closeable
import co.powersync.bucket.BucketStorage
import co.powersync.connectors.PowerSyncBackendConnector
import co.powersync.db.crud.CrudBatch
import co.powersync.db.crud.CrudEntry
import co.powersync.db.crud.CrudRow
import co.powersync.db.crud.CrudTransaction
import co.powersync.db.internal.SqlDatabase
import co.powersync.db.schema.Schema
import co.powersync.sync.SyncStatus
import co.powersync.sync.SyncStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.IO
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/**
 * A PowerSync managed database.
 *
 * Use one instance per database file.
 *
 * Use [PowerSyncDatabase.connect] to connect to the PowerSync service, to keep the local database in sync with the remote database.
 *
 * All changes to local tables are automatically recorded, whether connected or not. Once connected, the changes are uploaded.
 */
open class PowerSyncDatabase(
    driverFactory: DatabaseDriverFactory,
    /**
     * Schema used for the local database.
     */
    val schema: Schema,

    /**
     * Filename for the database.
     */
    val dbFilename: String

) : Closeable, ReadQueries, WriteQueries {
    override var closed: Boolean = false
    val driver: SqlDriver = driverFactory.createDriver(schema, dbFilename)
    val sqlDatabase: SqlDatabase = SqlDatabase(driver)
    private val bucketStorage: BucketStorage

    /**
     * The current sync status.
     */
    val currentStatus: SyncStatus = SyncStatus()

    private var syncStream: SyncStream? = null

    init {
        this.bucketStorage = BucketStorage(sqlDatabase)

        runBlocking {
            applySchema();
        }
    }

    private suspend fun applySchema() {
        val json = Json { encodeDefaults = true }
        val schemaJson = json.encodeToString(this.schema)
        println("Serialized app schema: $schemaJson")

        this.writeTransaction {
            execute("SELECT powersync_replace_schema(?)", listOf(schemaJson))
        }
    }

    suspend fun connect(connector: PowerSyncBackendConnector) {
        this.syncStream =
            SyncStream(this.bucketStorage,
                credentialsCallback = suspend { connector.getCredentialsCached() },
                invalidCredentialsCallback = suspend { },
                uploadCrud = suspend { connector.uploadData(this) },
                flow {

                })

        GlobalScope.launch(Dispatchers.IO) {
            syncStream!!.streamingSyncIteration()
        }
    }

    /**
     * Get a batch of crud data to upload.
     *
     * Returns null if there is no data to upload.
     *
     * Use this from the [PowerSyncBackendConnector.uploadData]` callback.
     *
     * Once the data have been successfully uploaded, call [CrudBatch.complete] before
     * requesting the next batch.
     *
     * Use [limit] to specify the maximum number of updates to return in a single
     * batch.
     *
     * This method does include transaction ids in the result, but does not group
     * data by transaction. One batch may contain data from multiple transactions,
     * and a single transaction may be split over multiple batches.
     */
    suspend fun getCrudBatch(limit: Int = 100): CrudBatch? {
        if (!bucketStorage.hasCrud()) {
            return null
        }

        val entries = getAll(
            "SELECT id, tx_id, data FROM ps_crud ORDER BY id ASC LIMIT ?",
            listOf((limit + 1).toLong()),
            mapper = { cursor ->
                CrudEntry.fromRow(
                    CrudRow(
                        id = cursor.getString(0)!!,
                        data = cursor.getString(1)!!,
                        txId = cursor.getLong(2)?.toInt()
                    )
                )
            }
        )

        if (entries.isEmpty()) {
            return null
        }

        val hasMore = entries.size > limit;
        if (hasMore) {
            entries.dropLast(entries.size - limit)
        }

        return CrudBatch(entries, hasMore, complete = { writeCheckpoint ->
            handleWriteCheckpoint(entries.last().clientId, writeCheckpoint)
        })
    }

    /**
     * Get the next recorded transaction to upload.
     *
     * Returns null if there is no data to upload.
     *
     * Use this from the [PowerSyncBackendConnector.uploadData]` callback.
     *
     * Once the data have been successfully uploaded, call [CrudTransaction.complete] before
     * requesting the next transaction.
     *
     * Unlike [getCrudBatch], this only returns data from a single transaction at a time.
     * All data for the transaction is loaded into memory.
     */

    suspend fun getNextCrudTransaction(): CrudTransaction? {
        return this.readTransaction {
            val first =
                getOptional(
                    "SELECT id, tx_id, data FROM ps_crud ORDER BY id ASC LIMIT 1",
                    mapper = { cursor ->
                        CrudEntry.fromRow(
                            CrudRow(
                                id = cursor.getString(0)!!,
                                txId = cursor.getLong(1)?.toInt(),
                                data = cursor.getString(2)!!
                            )
                        )
                    })
                    ?: return@readTransaction null


            val txId = first.transactionId
            val entries: List<CrudEntry>
            if (txId == null) {
                entries = listOf(first)
            } else {
                entries = getAll(
                    "SELECT id, tx_id, data FROM ps_crud WHERE tx_id = ? ORDER BY id ASC",
                    listOf(txId.toLong()),
                    mapper = { cursor ->
                        CrudEntry.fromRow(
                            CrudRow(
                                id = cursor.getString(0)!!,
                                txId = cursor.getLong(1)?.toInt(),
                                data = cursor.getString(2)!!,
                            )
                        )
                    })
            }

            return@readTransaction CrudTransaction(
                crud = entries, transactionId = txId,
                complete = { writeCheckpoint ->
                    handleWriteCheckpoint(entries.last().clientId, writeCheckpoint)
                }
            )
        }
    }

    private suspend fun handleWriteCheckpoint(lastTransactionId: Int, writeCheckpoint: String?) {
        writeTransaction {
            execute(
                "DELETE FROM ps_crud WHERE id <= ?",
                listOf(lastTransactionId.toLong()),
            )

            if (writeCheckpoint != null && bucketStorage.hasCrud()) {
                execute(
                    "UPDATE ps_buckets SET target_op = ? WHERE name='\$local'",
                    listOf(writeCheckpoint),
                )
            } else {
                execute(
                    "UPDATE ps_buckets SET target_op = ? WHERE name='\$local'",
                    listOf(bucketStorage.getMaxOpId()),
                )
            }
        }
    }

    suspend fun getPowersyncVersion(): String {
        return get(
            "SELECT powersync_rs_version()",
            mapper = { cursor ->
                cursor.getString(0)!!
            }
        )
    }

    override suspend fun <RowType : Any> get(
        sql: String,
        parameters: List<Any>?,
        mapper: (SqlCursor) -> RowType
    ): RowType {
        return sqlDatabase.get(sql, parameters, mapper)
    }

    override suspend fun <RowType : Any> getAll(
        sql: String,
        parameters: List<Any>?,
        mapper: (SqlCursor) -> RowType
    ): List<RowType> {
        return sqlDatabase.getAll(sql, parameters, mapper)
    }

    override suspend fun <RowType : Any> getOptional(
        sql: String,
        parameters: List<Any>?,
        mapper: (SqlCursor) -> RowType
    ): RowType? {
        return sqlDatabase.getOptional(sql, parameters, mapper)
    }

    override suspend fun <RowType : Any> watch(
        sql: String,
        parameters: List<Any>?,
        mapper: (SqlCursor) -> RowType
    ): Flow<RowType> {
        return sqlDatabase.watch(sql, parameters, mapper)
    }

    override suspend fun <R> readTransaction(bodyWithReturn: suspend SuspendingTransactionWithReturn<R>.() -> R): R {
        return sqlDatabase.readTransaction(bodyWithReturn)
    }

    override suspend fun execute(sql: String, parameters: List<Any>?): Long {
        return sqlDatabase.execute(sql, parameters)
    }

    override suspend fun writeTransaction(bodyNoReturn: suspend SuspendingTransactionWithoutReturn.() -> Unit) {
        return sqlDatabase.writeTransaction(bodyNoReturn)
    }

    override suspend fun close() {
        closed = true
    }
}