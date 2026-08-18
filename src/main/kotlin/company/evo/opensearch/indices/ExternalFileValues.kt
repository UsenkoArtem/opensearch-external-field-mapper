package company.evo.opensearch.indices

import dev.evo.persistent.BufferManagement
import dev.evo.persistent.FileDoesNotExistException
import dev.evo.persistent.hashmap.straight.StraightHashMapEnv
import dev.evo.persistent.hashmap.straight.StraightHashMapROEnv
import dev.evo.persistent.hashmap.straight.StraightHashMapRO_Int_Float
import dev.evo.persistent.hashmap.straight.StraightHashMapRO_Long_Float
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Int_Float
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Float
import org.apache.logging.log4j.LogManager
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference

enum class ExternalFieldKeyType {
    INT, LONG
}

interface ExternalFileValues : AutoCloseable {
    data class Provider(
        val dir: Path,
        val sharding: Boolean,
        val numShards: Int,
        val useMemorySegments: Boolean,
    ) : AutoCloseable {
        private val mapEnvs: Array<AtomicReference<StraightHashMapROEnv<*, *, *>?>> = Array(numShards) {
            AtomicReference<StraightHashMapROEnv<*, *, *>?>(null)
        }

        companion object {
            private val logger = LogManager.getLogger(Provider::class.java)

            private const val MAX_ATTEMPTS = 3
        }

        fun getValues(keyType: ExternalFieldKeyType, shardId: Int?): ExternalFileValues {
            val mapEnvRef = mapEnvs[shardId ?: 0]
            repeat(MAX_ATTEMPTS) {
                val env = mapEnvRef.get()
                    ?: openEnv(mapEnvRef, keyType, shardId)
                    ?: return EmptyFileValues
                try {
                    return when (keyType) {
                        ExternalFieldKeyType.INT -> {
                            IntFloatFileValues(
                                env.getCurrentMap() as StraightHashMapRO_Int_Float
                            )
                        }

                        ExternalFieldKeyType.LONG -> {
                            LongFloatFileValues(
                                env.getCurrentMap() as StraightHashMapRO_Long_Float
                            )
                        }
                    }
                } catch (e: FileDoesNotExistException) {
                    logger.warn(
                        "Cannot get a current map from $dir, the version file seems to be " +
                            "replaced, reopening the environment", e
                    )
                    mapEnvRef.compareAndSet(env, null)
                }
            }
            logger.error("Cannot get a current map from $dir after $MAX_ATTEMPTS attempts")
            return EmptyFileValues
        }

        private fun openEnv(
            mapEnvRef: AtomicReference<StraightHashMapROEnv<*, *, *>?>,
            keyType: ExternalFieldKeyType,
            shardId: Int?,
        ): StraightHashMapROEnv<*, *, *>? {
            val mapDir = if (shardId != null) {
                dir.resolve(shardId.toString())
            } else {
                dir
            }
            val mapEnvBuilder = when (keyType) {
                ExternalFieldKeyType.INT -> StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                ExternalFieldKeyType.LONG -> StraightHashMapEnv.Builder(StraightHashMapType_Long_Float)
            }
            val bufferManagement = if (useMemorySegments) {
                BufferManagement.MemorySegments
            } else {
                BufferManagement.Unsafe(true)
            }
            val newEnv = try {
                mapEnvBuilder
                    .bufferManagement(bufferManagement)
                    .openReadOnly(mapDir)
            } catch (_: FileDoesNotExistException) {
                return null
            }
            if (mapEnvRef.compareAndSet(null, newEnv)) {
                return newEnv
            }
            // Another thread already has set an environment
            newEnv.close()
            return mapEnvRef.get()
        }

        override fun close() {
            mapEnvs.forEach { env ->
                env.get()?.close()
            }
        }
    }

    // These are for testing purposes
    val version: Long
    fun refCount(): Long

    fun get(key: Long, defaultValue: Double): Double

    fun contains(key: Long): Boolean
}

object EmptyFileValues : ExternalFileValues {
    override val version: Long = 0

    override fun refCount(): Long {
        return 1
    }

    override fun get(key: Long, defaultValue: Double): Double {
        return defaultValue
    }

    override fun contains(key: Long): Boolean {
        return false
    }

    override fun close() {}
}

class LongFloatFileValues(
    private val map: StraightHashMapRO_Long_Float
) : ExternalFileValues {
    override val version: Long = map.version

    override fun refCount(): Long {
        return map.refCount()
    }

    override fun get(key: Long, defaultValue: Double): Double {
        val v = map.get(key, Float.NaN)
        if (v.isNaN()) {
            return defaultValue
        }
        return v.toDouble()
    }

    override fun contains(key: Long): Boolean {
        return map.contains(key)
    }

    override fun close() {
        map.close()
    }
}

class IntFloatFileValues(
    private val map: StraightHashMapRO_Int_Float
) : ExternalFileValues {
    override val version: Long = map.version

    override fun refCount(): Long {
        return map.refCount()
    }

    override fun get(key: Long, defaultValue: Double): Double {
        if (key > Int.MAX_VALUE) {
            return defaultValue
        }
        val v = map.get(key.toInt(), Float.NaN)
        if (v.isNaN()) {
            return defaultValue
        }
        return v.toDouble()
    }

    override fun contains(key: Long): Boolean {
        return map.contains(key.toInt())
    }

    override fun close() {
        map.close()
    }
}
