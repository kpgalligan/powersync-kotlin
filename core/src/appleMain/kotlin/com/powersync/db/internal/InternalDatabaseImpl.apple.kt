package com.powersync.db.internal

import kotlin.native.concurrent.Worker

public actual fun currentThreadId(): Long = Worker.current.id.toLong()