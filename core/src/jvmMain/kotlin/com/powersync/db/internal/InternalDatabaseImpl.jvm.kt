package com.powersync.db.internal

public actual fun currentThreadId(): Long = Thread.currentThread().id