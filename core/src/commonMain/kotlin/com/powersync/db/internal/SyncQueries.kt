package com.powersync.db.internal

import app.cash.sqldelight.db.SqlCursor
import kotlinx.coroutines.flow.Flow

internal interface SyncQueries {
    /**
     * Execute a write query (INSERT, UPDATE, DELETE)
     */
    fun execute(
        sql: String,
        parameters: List<Any?>? = listOf(),
    ): Long

    /**
     * Execute a read-only (SELECT) query and return a single result.
     * If there is no result, throws an [IllegalArgumentException].
     * See [getOptional] for queries where the result might be empty.
     */
    fun <RowType : Any> get(
        sql: String,
        parameters: List<Any?>? = listOf(),
        mapper: (SqlCursor) -> RowType,
    ): RowType

    /**
     * Execute a read-only (SELECT) query and return the results.
     */
    fun <RowType : Any> getAll(
        sql: String,
        parameters: List<Any?>? = listOf(),
        mapper: (SqlCursor) -> RowType,
    ): List<RowType>

    /**
     * Execute a read-only (SELECT) query and return a single optional result.
     */
    fun <RowType : Any> getOptional(
        sql: String,
        parameters: List<Any?>? = listOf(),
        mapper: (SqlCursor) -> RowType,
    ): RowType?

    /**
     * Execute a read-only (SELECT) query every time the source tables are modified and return the results as a List in [Flow].
     */
    fun <RowType : Any> watch(
        sql: String,
        parameters: List<Any?>? = listOf(),
        mapper: (SqlCursor) -> RowType,
    ): Flow<List<RowType>>

    fun <R> writeTransaction(callback: (SyncQueries) -> R): R

    fun <R> readTransaction(callback: (SyncQueries) -> R): R
}