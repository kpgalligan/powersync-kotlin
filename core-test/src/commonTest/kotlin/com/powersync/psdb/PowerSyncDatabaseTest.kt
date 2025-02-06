package com.powersync.psdb

import com.powersync.DatabaseDriverFactory
import com.powersync.PowerSyncDatabase
import com.powersync.db.schema.Column
import com.powersync.db.schema.Schema
import com.powersync.db.schema.Table
import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

expect abstract class RobolectricTest()

expect fun cleanupDb()

class PowerSyncDatabaseTest : RobolectricTest() {
    private lateinit var database: PowerSyncDatabase

    @BeforeTest
    fun testOk() {
        database = PowerSyncDatabase(
            factory = factory,
            schema = Schema(
                Table(name = "users", columns = listOf(Column.text("name"), Column.text("email")))
            ),
            dbFilename = "testdb"
        )
    }

    @AfterTest
    fun tearDown() {
        runBlocking {
            database.disconnectAndClear()
        }
        cleanupDb()
    }

    @Test
    fun testInsertAndGet() {
        runBlocking {
            database.execute(
                sql = "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                parameters = listOf("1", "Test User", "test@example.com")
            )

            val user = database.get(
                sql = "SELECT id, name, email FROM users WHERE id = ?",
                parameters = listOf("1")
            ) { cursor ->
                listOf(
                    cursor.getString(0)!!,
                    cursor.getString(1)!!,
                    cursor.getString(2)!!
                )
            }

            assertEquals(user[0], "1")
            assertEquals(user[1], "Test User")
            assertEquals(user[2], "test@example.com")
        }
    }
}

expect val factory: DatabaseDriverFactory