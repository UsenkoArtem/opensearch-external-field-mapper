package company.evo.opensearch.indices

import company.evo.opensearch.plugin.mapper.ExternalFileMapperPlugin

import dev.evo.persistent.BufferManagement
import dev.evo.persistent.hashmap.straight.StraightHashMapEnv
import dev.evo.persistent.hashmap.straight.StraightHashMapType_Long_Float

import org.opensearch.common.util.io.IOUtils
import org.opensearch.plugins.Plugin
import org.opensearch.test.OpenSearchSingleNodeTestCase

import java.nio.file.Files
import java.nio.file.Path

class ExternalFileValuesProviderTests : OpenSearchSingleNodeTestCase() {


    override fun getPlugins(): Collection<Class<out Plugin>> {
        return pluginList(ExternalFileMapperPlugin::class.java)
    }

    private fun openWritableEnv(mapDir: Path) = StraightHashMapEnv
        .Builder(StraightHashMapType_Long_Float)
        .initialEntries(20)
        .bufferManagement(BufferManagement.MemorySegments)
        .open(mapDir.also { Files.createDirectories(it) })

    private fun getValue(provider: ExternalFileValues.Provider, key: Long): Double {
        return provider.getValues(ExternalFieldKeyType.LONG, 0).use { values ->
            values.get(key, Double.NaN)
        }
    }

    fun testRecoversAfterVersionFileIsReplaced() {
        val dir = createTempDir()
        val mapDir = dir.resolve("0")

        var env = openWritableEnv(mapDir)
        var map = env.openMap()
        map.put(1L, 1.1F)

        val provider = ExternalFileValues.Provider(dir, true, 1, true)
        assertEquals(1.1, getValue(provider, 1L), 1e-6)

        // Bump the version, a recreated directory starts from zero again
        val bumpedMap = env.copyMap(map)
        map.close()
        env.commit(bumpedMap)
        map = bumpedMap
        map.close()
        env.close()

        IOUtils.rm(mapDir)
        env = openWritableEnv(mapDir)
        map = env.openMap()
        map.put(2L, 2.2F)

        assertEquals(2.2, getValue(provider, 2L), 1e-6)
        assertTrue(getValue(provider, 1L).isNaN())

        provider.close()
        map.close()
        env.close()
    }

    fun testReadsAgainAfterReturningDefaults() {
        val dir = createTempDir()
        val mapDir = dir.resolve("0")

        var env = openWritableEnv(mapDir)
        var map = env.openMap()
        map.put(1L, 1.1F)
        val version = map.version
        map.close()
        env.close()

        Files.delete(mapDir.resolve("hashmap_$version.data"))

        val provider = ExternalFileValues.Provider(dir, true, 1, true)
        assertTrue(getValue(provider, 1L).isNaN())

        IOUtils.rm(mapDir)
        env = openWritableEnv(mapDir)
        map = env.openMap()
        map.put(1L, 3.3F)

        assertEquals(3.3, getValue(provider, 1L), 1e-6)

        provider.close()
        map.close()
        env.close()
    }

    fun testReturnsDefaultsWhenDirectoryDoesNotExist() {
        val dir = createTempDir()
        val provider = ExternalFileValues.Provider(dir, true, 1, true)

        assertTrue(getValue(provider, 1L).isNaN())

        provider.close()
    }
}
