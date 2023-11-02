package co.powersync.kotlin.bucket

import android.content.ContentValues
import android.database.Cursor
import io.requery.android.database.sqlite.SQLiteDatabase
import java.util.UUID

class KotlinBucketStorageAdapter(
    private val database: SQLiteDatabase,
    ): BucketStorageAdapter()  {
    companion object {
        private const val MAX_OP_ID = "9223372036854775807"
    }

    private val tableNames: MutableSet<String> = mutableSetOf()

    // TODO thread safe?!
    private var _hasCompletedSync = false
    // TODO thread safe?!
    private var pendingBucketDeletes = false;
    override suspend fun init() {
        _hasCompletedSync = false

        readTableNames()
    }

    private fun readTableNames() {
        tableNames.clear()
        // Query to get existing table names
        val query = "SELECT name FROM sqlite_master WHERE type='table' AND name GLOB 'ps_data_*'"
        val cursor: Cursor = database.rawQuery(query, null)
        val nameIndex = cursor.getColumnIndex("name")
        while (cursor.moveToNext()) {
            val name = cursor.getString(nameIndex)
            tableNames.add(name)
        }

        cursor.close()
    }

    override suspend fun saveSyncData(batch: SyncDataBatch) {
        TODO("Not yet implemented")
    }

    override suspend fun removeBuckets(buckets: Array<String>) {
        for(bucket in buckets){
            deleteBucket(bucket)
        }
    }

    suspend fun deleteBucket(bucket: String){
        // Delete a bucket, but allow it to be re-created.
        // To achieve this, we rename the bucket to a new temp name, and change all ops to remove.
        // By itself, this new bucket would ensure that the previous objects are deleted if they contain no more references.
        // If the old bucket is re-added, this new bucket would have no effect.
        val newName = "\$delete_${bucket}_${UUID.randomUUID()}";
        println("Deleting bucket  $bucket");

        database.beginTransaction()

        try {
            val values = ContentValues().apply {
                put("op", OpTypeEnum.REMOVE.toString())
                put("data", "NULL")
            }

            val where = "op=${OpTypeEnum.PUT} AND superseded=0 AND bucket=?"

            val args = arrayOf(bucket)

            val res = database.update("ps_oplog", values, where, args);

            // Rename the bucket to the new name
            val res2 = database.update("ps_oplog",
                ContentValues().apply {
                put("bucket", newName)
            },
                "bucket=?",
                arrayOf( bucket)
            )

            val res3 = database.delete("ps_buckets", "name = ?", arrayOf(bucket))

            val res4Cursor = database.rawQuery(
                "INSERT INTO ps_buckets(name, pending_delete, last_op) SELECT ?, 1, IFNULL(MAX(op_id), 0) FROM ps_oplog WHERE bucket = ?" ,
                arrayOf(newName, newName)
            )


            database.setTransactionSuccessful();
        } finally {
            database.endTransaction();
        }
        println("done deleting bucket")
        pendingBucketDeletes = true;
    }

    override suspend fun setTargetCheckpoint(checkpoint: Checkpoint) {
        // No-op for now
    }

    override fun startSession() {
        // Do nothing, yet
    }

    override suspend fun getBucketStates(): Array<BucketState> {
        val buckets = mutableSetOf<BucketState>()
        val query = "SELECT name as bucket, cast(last_op as TEXT) as op_id FROM ps_buckets WHERE pending_delete = 0"
        val cursor: Cursor = database.rawQuery(query, null)
        val bucketIndex = cursor.getColumnIndex("bucket")
        val opIdIndex = cursor.getColumnIndex("op_id")
        while (cursor.moveToNext()) {
            val bucket = cursor.getString(bucketIndex)
            val opId = cursor.getString(opIdIndex)
            buckets.add(BucketState(bucket, opId))
        }

        cursor.close()

        // TODO maybe we can stick to set? or list TODO x2 read up on list vs map vs set in Kotlin world
        return buckets.toTypedArray()
    }

    override suspend fun syncLocalDatabase(checkpoint: Checkpoint): SyncLocalDatabaseResult {
        TODO("Not yet implemented")
    }

    override suspend fun hasCrud(): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun getCrudBatch(limit: Int?): CrudBatch? {
        TODO("Not yet implemented")
    }

    override suspend fun hasCompletedSync(): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun updateLocalTarget(cb: suspend () -> String): Boolean {
        TODO("Not yet implemented")
    }

    override suspend fun autoCompact() {
        TODO("Not yet implemented")
    }

    override suspend fun forceCompact() {
        TODO("Not yet implemented")
    }

    override fun getMaxOpId(): String {
        return MAX_OP_ID
    }
}