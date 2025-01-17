package com.powersync.db.internal

import app.cash.sqldelight.ExecutableQuery
import app.cash.sqldelight.Query
import app.cash.sqldelight.coroutines.asFlow
import app.cash.sqldelight.coroutines.mapToList
import app.cash.sqldelight.db.QueryResult
import app.cash.sqldelight.db.SqlCursor
import app.cash.sqldelight.db.SqlDriver
import app.cash.sqldelight.db.SqlPreparedStatement
import com.persistence.PowersyncQueries
import com.powersync.PsSqlDriver
import com.powersync.persistence.PsDatabase
import com.powersync.utils.JsonUtil
import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString

@OptIn(FlowPreview::class)
internal class InternalDatabaseImpl(
    override val driver: PsSqlDriver,
    private val scope: CoroutineScope,
) : InternalDatabase {
    override val transactor: PsDatabase = PsDatabase(driver)
    override val queries: PowersyncQueries = transactor.powersyncQueries
    private val transaction =
        object : PowerSyncTransaction {
            override suspend fun execute(
                sql: String,
                parameters: List<Any?>?,
            ): Long = this@InternalDatabaseImpl.execute(sql, parameters ?: emptyList())

            override suspend fun <RowType : Any> get(
                sql: String,
                parameters: List<Any?>?,
                mapper: (SqlCursor) -> RowType,
            ): RowType = this@InternalDatabaseImpl.get(sql, parameters ?: emptyList(), mapper)

            override suspend fun <RowType : Any> getAll(
                sql: String,
                parameters: List<Any?>?,
                mapper: (SqlCursor) -> RowType,
            ): List<RowType> = this@InternalDatabaseImpl.getAll(sql, parameters ?: emptyList(), mapper)

            override suspend fun <RowType : Any> getOptional(
                sql: String,
                parameters: List<Any?>?,
                mapper: (SqlCursor) -> RowType,
            ): RowType? = this@InternalDatabaseImpl.getOptional(sql, parameters ?: emptyList(), mapper)
        }

    internal class SerialPowerSyncTransaction<R> : PowerSyncTransaction {
        internal val currentOperation = atomic<Operation<*>?>(null)

        internal fun nextOperation(): Operation<*>{
            var op = currentOperation.value
            while (op == null){
                op = currentOperation.value
            }
            return op
        }

        private suspend fun <R : Any> waitOnResult(operation: Operation<R>): R {
            var result: R? = operation.operationResult
            while (result == null) {
                delay(100)
                result = operation.operationResult
            }
            return result
        }

        private fun setNullOnly(op: Operation<*>) {
            if (!currentOperation.compareAndSet(null, op)) {
                throw IllegalStateException("Operation should be null but is ${currentOperation.value}")
            }
        }

        override suspend fun execute(
            sql: String,
            parameters: List<Any?>?,
        ): Long {
            val operation = Operation.Execute(sql, parameters)
            setNullOnly(operation)
            return waitOnResult(operation)
        }//withContext(serialContext) { this@InternalDatabaseImpl.execute(sql, parameters ?: emptyList()) }

        override suspend fun <RowType : Any> get(
            sql: String,
            parameters: List<Any?>?,
            mapper: (SqlCursor) -> RowType,
        ): RowType {
            val operation = Operation.Get(sql, parameters, mapper)
            setNullOnly(operation)
            return waitOnResult(operation)
        }//= withContext(serialContext) { this@InternalDatabaseImpl.get(sql, parameters ?: emptyList(), mapper)}

        override suspend fun <RowType : Any> getAll(
            sql: String,
            parameters: List<Any?>?,
            mapper: (SqlCursor) -> RowType,
        ): List<RowType> {
            val operation = Operation.GetAll(sql, parameters, mapper)
            setNullOnly(operation)
            return waitOnResult(operation)
        }// = withContext(serialContext) { this@InternalDatabaseImpl.getAll(sql, parameters ?: emptyList(), mapper)}

        override suspend fun <RowType : Any> getOptional(
            sql: String,
            parameters: List<Any?>?,
            mapper: (SqlCursor) -> RowType,
        ): RowType? {
            val operation = Operation.GetAll(sql, parameters, mapper)
            setNullOnly(operation)
            return waitOnResult(operation).firstOrNull()
        }//= withContext(serialContext) { this@InternalDatabaseImpl.getOptional(sql, parameters ?: emptyList(), mapper)}

        sealed interface Operation<R : Any> {
            val operationResult: R?
            fun runSync(driver: SqlDriver)

            fun <T : Any> createQuery(
                driver: SqlDriver,
                query: String,
                mapper: (SqlCursor) -> T,
                parameters: Int = 0,
                binders: (SqlPreparedStatement.() -> Unit)? = null,
            ): ExecutableQuery<T> =
                object : ExecutableQuery<T>(mapper) {
                    override fun <R> execute(mapper: (SqlCursor) -> QueryResult<R>): QueryResult<R> =
                        driver.executeQuery(null, query, mapper, parameters, binders)
                }

            data class Execute(
                val sql: String, val parameters: List<Any?>?,
            ) : Operation<Long> {
                val result: AtomicRef<Long?> = atomic<Long?>(null)
                override val operationResult: Long?
                    get() = result.value

                override fun runSync(driver: SqlDriver) {
                    val numParams = parameters?.size ?: 0
                    val r = driver
                        .execute(
                            identifier = null,
                            sql = sql,
                            parameters = numParams,
                            binders = getBindersFromParams(parameters),
                        ).value
                    result.value = r
                }
            }

            data class Get<RowType : Any>(
                val sql: String,
                val parameters: List<Any?>?,
                val mapper: (SqlCursor) -> RowType,

                ) : Operation<RowType> {
                val result: AtomicRef<RowType?> = atomic<RowType?>(null)
                override val operationResult: RowType?
                    get() = result.value

                override fun runSync(driver: SqlDriver) {
                    val r =
                        createQuery(
                            driver = driver,
                            query = sql,
                            parameters = parameters?.size ?: 0,
                            binders = getBindersFromParams(parameters),
                            mapper = mapper,
                        ).executeAsOneOrNull()
                    requireNotNull(r) { "Query returned no result" }
                    result.value = r
                }
            }

            data class GetAll<RowType : Any>(
                val sql: String,
                val parameters: List<Any?>?,
                val mapper: (SqlCursor) -> RowType,
            ) : Operation<List<RowType>> {
                val result: AtomicRef<List<RowType>?> = atomic<List<RowType>?>(null)
                override val operationResult: List<RowType>?
                    get() = result.value

                override fun runSync(driver: SqlDriver) {
                    result.value = createQuery(
                        driver = driver,
                        query = sql,
                        parameters = parameters?.size ?: 0,
                        binders = getBindersFromParams(parameters),
                        mapper = mapper,
                    ).executeAsList()
                }
            }

            data class GetOptional<RowType : Any>(
                val sql: String,
                val parameters: List<Any?>?,
                val mapper: (SqlCursor) -> RowType,
            ) : Operation<List<RowType>> {
                val result = atomic<List<RowType>?>(null)
                override val operationResult: List<RowType>?
                    get() = result.value

                override fun runSync(driver: SqlDriver) {
                    result.value = createQuery(
                        driver = driver,
                        query = sql,
                        parameters = parameters?.size ?: 0,
                        binders = getBindersFromParams(parameters),
                        mapper = mapper,
                    ).executeAsList()
                }
            }
        }
    }

    companion object {
        const val POWERSYNC_TABLE_MATCH: String = "(^ps_data__|^ps_data_local__)"
        const val DEFAULT_WATCH_THROTTLE_MS: Long = 30L
    }

    init {
        scope.launch {
            val accumulatedUpdates = mutableSetOf<String>()
            tableUpdates()
//               Debounce will discard any events which occur inside the debounce window
//               This will accumulate those table updates
                .onEach { tables -> accumulatedUpdates.addAll(tables) }
                .debounce(DEFAULT_WATCH_THROTTLE_MS)
                .collect {
                    val dataTables = accumulatedUpdates.map { toFriendlyTableName(it) }.filter { it.isNotBlank() }
                    driver.notifyListeners(queryKeys = dataTables.toTypedArray())
                    accumulatedUpdates.clear()
                }
        }
    }

    override suspend fun execute(
        sql: String,
        parameters: List<Any?>?,
    ): Long {
        val numParams = parameters?.size ?: 0
        println("InternalDatabaseImpl-execute-start")
        val result = driver
            .execute(
                identifier = null,
                sql = sql,
                parameters = numParams,
                binders = getBindersFromParams(parameters),
            ).value
        println("InternalDatabaseImpl-execute-end")
        return result
    }

    override suspend fun <RowType : Any> get(
        sql: String,
        parameters: List<Any?>?,
        mapper: (SqlCursor) -> RowType,
    ): RowType {
        val result =
            this
                .createQuery(
                    query = sql,
                    parameters = parameters?.size ?: 0,
                    binders = getBindersFromParams(parameters),
                    mapper = mapper,
                ).executeAsOneOrNull()
        return requireNotNull(result) { "Query returned no result" }
    }

    override suspend fun <RowType : Any> getAll(
        sql: String,
        parameters: List<Any?>?,
        mapper: (SqlCursor) -> RowType,
    ): List<RowType> =
        this
            .createQuery(
                query = sql,
                parameters = parameters?.size ?: 0,
                binders = getBindersFromParams(parameters),
                mapper = mapper,
            ).executeAsList()

    override suspend fun <RowType : Any> getOptional(
        sql: String,
        parameters: List<Any?>?,
        mapper: (SqlCursor) -> RowType,
    ): RowType? =
        this
            .createQuery(
                query = sql,
                parameters = parameters?.size ?: 0,
                binders = getBindersFromParams(parameters),
                mapper = mapper,
            ).executeAsOneOrNull()

    override fun <RowType : Any> watch(
        sql: String,
        parameters: List<Any?>?,
        mapper: (SqlCursor) -> RowType,
    ): Flow<List<RowType>> {
        val tables =
            getSourceTables(sql, parameters)
                .map { toFriendlyTableName(it) }
                .filter { it.isNotBlank() }
                .toSet()
        return watchQuery(
            query = sql,
            parameters = parameters?.size ?: 0,
            binders = getBindersFromParams(parameters),
            mapper = mapper,
            tables = tables,
        ).asFlow().mapToList(scope.coroutineContext)
    }

    private fun <T : Any> createQuery(
        query: String,
        mapper: (SqlCursor) -> T,
        parameters: Int = 0,
        binders: (SqlPreparedStatement.() -> Unit)? = null,
    ): ExecutableQuery<T> =
        object : ExecutableQuery<T>(mapper) {
            override fun <R> execute(mapper: (SqlCursor) -> QueryResult<R>): QueryResult<R> =
                driver.executeQuery(null, query, mapper, parameters, binders)
        }

    private fun <T : Any> watchQuery(
        query: String,
        mapper: (SqlCursor) -> T,
        parameters: Int = 0,
        binders: (SqlPreparedStatement.() -> Unit)? = null,
        tables: Set<String> = setOf(),
    ): Query<T> =
        object : Query<T>(mapper) {
            override fun <R> execute(mapper: (SqlCursor) -> QueryResult<R>): QueryResult<R> =
                driver.executeQuery(null, query, mapper, parameters, binders)

            override fun addListener(listener: Listener) {
                driver.addListener(queryKeys = tables.toTypedArray(), listener = listener)
            }

            override fun removeListener(listener: Listener) {
                driver.removeListener(queryKeys = tables.toTypedArray(), listener = listener)
            }
        }

    override suspend fun <R> readTransaction(callback: suspend (PowerSyncTransaction) -> R): R =
        transactor.transactionWithResult(noEnclosing = true) {
            println("readTransaction-runBlocking")
            runBlocking {
                callback(transaction)
            }
        }

    override suspend fun <R> writeTransaction(callback: suspend (PowerSyncTransaction) -> R): R {
        return withContext(Dispatchers.IO) {
            val transacter = SerialPowerSyncTransaction<R>()
            val job = launch {
                callback(transacter)
            }

            println("serial-transactionWithResult-beforeStart")
            val result = transactor.transactionWithResult(noEnclosing = true) {
                println("serial-transactionWithResult-start-${currentThreadId()}")
                var opResult:Any? = null
                while (job.isActive){
                    val nextOperation = transacter.nextOperation()
                    println("serial-transactionWithResult-running-${currentThreadId()}")
                    opResult = nextOperation.runSync(driver)
                    nextOperation.operationResult
                }
                println("serial-transactionWithResult-end-${currentThreadId()}")
                opResult
            } as R

            return@withContext result
        }
    }

    // Register callback for table updates
    private fun tableUpdates(): Flow<List<String>> = driver.tableUpdates()

    // Register callback for table updates on a specific table
    override fun updatesOnTable(tableName: String): Flow<Unit> = driver.updatesOnTable(tableName)

    private fun toFriendlyTableName(tableName: String): String {
        val regex = POWERSYNC_TABLE_MATCH.toRegex()
        if (regex.containsMatchIn(tableName)) {
            return tableName.replace(regex, "")
        }
        return tableName
    }

    private fun getSourceTables(
        sql: String,
        parameters: List<Any?>?,
    ): Set<String> {
        val rows =
            createQuery(
                query = "EXPLAIN $sql",
                parameters = parameters?.size ?: 0,
                binders = getBindersFromParams(parameters),
                mapper = {
                    ExplainQueryResult(
                        addr = it.getString(0)!!,
                        opcode = it.getString(1)!!,
                        p1 = it.getLong(2)!!,
                        p2 = it.getLong(3)!!,
                        p3 = it.getLong(4)!!,
                    )
                },
            ).executeAsList()

        val rootPages = mutableListOf<Long>()
        for (row in rows) {
            if ((row.opcode == "OpenRead" || row.opcode == "OpenWrite") && row.p3 == 0L && row.p2 != 0L) {
                rootPages.add(row.p2)
            }
        }
        val params = listOf(JsonUtil.json.encodeToString(rootPages))
        val tableRows =
            createQuery(
                "SELECT tbl_name FROM sqlite_master WHERE rootpage IN (SELECT json_each.value FROM json_each(?))",
                parameters = params.size,
                binders = {
                    bindString(0, params[0])
                },
                mapper = { it.getString(0)!! },
            ).executeAsList()

        return tableRows.toSet()
    }

    override fun getExistingTableNames(tableGlob: String): List<String> {
        val existingTableNames =
            createQuery(
                "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB ?",
                parameters = 1,
                binders = {
                    bindString(0, tableGlob)
                },
                mapper = { cursor ->
                    cursor.getString(0)!!
                },
            ).executeAsList()
        return existingTableNames
    }

    internal data class ExplainQueryResult(
        val addr: String,
        val opcode: String,
        val p1: Long,
        val p2: Long,
        val p3: Long,
    )
}

internal fun getBindersFromParams(parameters: List<Any?>?): (SqlPreparedStatement.() -> Unit)? {
    if (parameters.isNullOrEmpty()) {
        return null
    }
    return {
        parameters.forEachIndexed { index, parameter ->
            when (parameter) {
                is Boolean -> bindBoolean(index, parameter)
                is String -> bindString(index, parameter)
                is Long -> bindLong(index, parameter)
                is Double -> bindDouble(index, parameter)
                is ByteArray -> bindBytes(index, parameter)
                else -> {
                    if (parameter != null) {
                        throw IllegalArgumentException("Unsupported parameter type: ${parameter::class}, at index $index")
                    }
                }
            }
        }
    }
}

public expect fun currentThreadId(): Long