/*
* Copyright 2017 Alexander Koval
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package company.evo.opensearch.index.mapper.external

import company.evo.opensearch.indices.*

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.SortedNumericDocValues
import org.apache.lucene.search.Query

import org.opensearch.core.index.Index
import org.opensearch.index.fielddata.FieldData
import org.opensearch.index.fielddata.IndexFieldData
import org.opensearch.index.fielddata.IndexNumericFieldData
import org.opensearch.index.fielddata.LeafNumericFieldData
import org.opensearch.index.fielddata.ScriptDocValues
import org.opensearch.index.fielddata.SortedBinaryDocValues
import org.opensearch.index.fielddata.SortedNumericDoubleValues
import org.opensearch.index.IndexSettings
import org.opensearch.index.mapper.FieldMapper
import org.opensearch.index.mapper.MappedFieldType
import org.opensearch.index.mapper.Mapper
import org.opensearch.index.mapper.ParametrizedFieldMapper
import org.opensearch.index.mapper.ParseContext
import org.opensearch.index.mapper.TextSearchInfo
import org.opensearch.index.mapper.ValueFetcher
import org.opensearch.index.query.QueryShardContext
import org.opensearch.index.query.QueryShardException
import org.opensearch.search.aggregations.support.CoreValuesSourceType
import org.opensearch.search.aggregations.support.ValuesSourceType
import org.opensearch.search.lookup.SearchLookup

import java.util.function.Supplier

class ExternalFileFieldMapper private constructor(
    simpleName: String,
    mappedFieldType: MappedFieldType,
    multiFields: MultiFields,
    copyTo: CopyTo?,
    builder: Builder
) : ParametrizedFieldMapper(
    simpleName,
    mappedFieldType,
    multiFields,
    copyTo
) {
    private val mapName: String = builder.mapName.value
    private val keyFieldName: String = builder.keyFieldName.value
    private val sharding: Boolean = builder.sharding.value

    companion object {
        @JvmStatic
        val CONTENT_TYPE = "external_file"

        @JvmStatic
        private fun toType(fieldMapper: FieldMapper): ExternalFileFieldMapper {
            return fieldMapper as ExternalFileFieldMapper
        }
    }

    class TypeParser : Mapper.TypeParser {
        override fun parse(
            name: String,
            node: MutableMap<String, Any?>,
            parserContext: Mapper.TypeParser.ParserContext
        ): Mapper.Builder<*> {
            val queryShardContext = try {
                parserContext.queryShardContextSupplier().get()
            } catch (e: java.lang.UnsupportedOperationException) {
                null
            }
            return Builder(name, queryShardContext?.indexSettings).apply {
                parse(name, parserContext, node)
            }
        }
    }

    override fun contentType() : String {
        return CONTENT_TYPE
    }

    override fun parseCreateField(context: ParseContext) {
        // Just ignore field values
    }

    override fun getMergeBuilder(): ParametrizedFieldMapper.Builder {
        return Builder(simpleName(), null).init(this)
    }

    class Builder(
        name: String,
        private val indexSettings: IndexSettings?
    ) : ParametrizedFieldMapper.Builder(name) {
        val mapName: Parameter<String> = Parameter.stringParam(
            "map_name", true, { m -> toType(m).mapName }, null
        )
        val keyFieldName: Parameter<String> = Parameter.stringParam(
            "key_field", true, { m -> toType(m).keyFieldName }, null
        )
        val sharding: Parameter<Boolean> = Parameter.boolParam(
            "sharding", true, { m -> toType(m).sharding }, false
        )

        // val hasDocValues: Parameter<Boolean> = Parameter.boolParam(
        //     "doc_values", false, { m -> toType(m).hasDocValues }, true
        // )

        // Ignored, but keep it to work with old indexes
        val scalingFactor: Parameter<Float> = Parameter.floatParam(
            "scaling_factor", true, { 0.0F }, 0.0F
        )

        override fun getParameters(): MutableList<Parameter<*>> {
            return mutableListOf(mapName, keyFieldName, sharding, scalingFactor)
        }

        override fun build(context: BuilderContext): ExternalFileFieldMapper {
            val fullFieldName = buildFullName(context)

            // There is no index when putting a template or cloning from another one
            if (indexSettings != null) {
                val indexName = indexSettings.index.name
                val numShards = indexSettings.indexMetadata.numberOfShards
                ExternalFileService.instance.addFile(
                    indexName,
                    fullFieldName,
                    mapName.value,
                    sharding.value,
                    numShards
                )
            }

            return ExternalFileFieldMapper(
                name,
                ExternalFileFieldType(
                    fullFieldName, mapName.value, keyFieldName.value, sharding.value
                ),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            )
        }
    }

    class ExternalFileFieldType(
        name: String,
        private val mapName: String,
        private val keyFieldName: String,
        private val sharding: Boolean
    ) : MappedFieldType(name, false, false, true, TextSearchInfo.NONE, emptyMap()) {

        override fun typeName(): String {
            return CONTENT_TYPE
        }

        override fun termQuery(value: Any, context: QueryShardContext?): Query {
            throw QueryShardException(
                context,
                "ExternalFile field type does not support search queries"
            )
        }

        override fun existsQuery(context: QueryShardContext): Query {
            throw QueryShardException(
                context,
                "ExternalFile field type does not support exists queries"
            )
        }

        override fun valueFetcher(
            context: QueryShardContext,
            searchLookup: SearchLookup,
            format: String?,
        ): ValueFetcher {
            // TODO: Implement fetching values
            throw QueryShardException(
                context,
                "ExternalField field type does not support fetching values"
            )
        }

        override fun fielddataBuilder(
            fullyQualifiedIndexName: String,
            searchLookupSupplier: Supplier<SearchLookup>
        ): IndexFieldData.Builder {
            return IndexFieldData.Builder { cache, breakerService ->
                val searchLookup = searchLookupSupplier.get()
                val mapperService = searchLookup.doc().mapperService();
                val shardId = searchLookup.shardId()

                val keyFieldType = mapperService.fieldType(keyFieldName)
                val externalFieldKeyType = when (keyFieldType.typeName()) {
                    "integer" -> ExternalFieldKeyType.INT
                    "long" -> ExternalFieldKeyType.LONG
                    else -> {
                        throw IllegalStateException(
                            "Unsupported field type for [$keyFieldName] field: ${keyFieldType.typeName()}. " +
                                "Supported types: integer, long"
                        )
                    }
                }
                val keyFieldData = keyFieldType
                    .fielddataBuilder(fullyQualifiedIndexName, searchLookupSupplier)
                    .build(cache, breakerService) as? IndexNumericFieldData
                    ?: throw IllegalStateException("[$keyFieldName] field must be numeric")

                ExternalFileFieldData(
                    name(),
                    keyFieldData,
                    ExternalFileService.instance.getValues(mapName, externalFieldKeyType, if (sharding) shardId else null)
                )
            }
        }
    }

    class ExternalFileFieldData(
        private val fieldName: String,
        private val keyFieldData: IndexNumericFieldData,
        private val values: ExternalFileValues
    ) : IndexNumericFieldData() {

        companion object {
            private val DEFAULT_VALUE = Double.NaN
        }

        class AtomicNumericKeyFieldData(
                private val values: ExternalFileValues,
                private val keyFieldData: LeafNumericFieldData
        ) : LeafNumericFieldData {

            class Values(
                    private val values: ExternalFileValues,
                    private val keys: SortedNumericDocValues
            ) : SortedNumericDoubleValues() {

                private var value = DEFAULT_VALUE

                override fun advanceExact(target: Int): Boolean {
                    return if (keys.advanceExact(target)) {
                        val key = keys.nextValue()
                        val v = values.get(key, DEFAULT_VALUE)
                        if (!v.isNaN()) {
                            value = v
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }

                override fun nextValue() = value

                override fun docValueCount() = 1
            }

            override fun getDoubleValues(): SortedNumericDoubleValues {
                return Values(values, keyFieldData.longValues)
            }

            override fun getLongValues(): SortedNumericDocValues {
                return FieldData.castToLong(doubleValues)
            }

            override fun getScriptValues(): ScriptDocValues.Doubles {
                return ScriptDocValues.Doubles(Values(values, keyFieldData.longValues))
            }

            override fun getBytesValues(): SortedBinaryDocValues {
                return FieldData.toString(doubleValues)
            }

            override fun ramBytesUsed(): Long {
                // TODO Calculate ram used
                return 0
            }

            override fun close() {}
        }

        override fun getValuesSourceType(): ValuesSourceType {
            return CoreValuesSourceType.NUMERIC
        }

        override fun sortRequiresCustomComparator(): Boolean {
            return true
        }

        override fun getNumericType(): IndexNumericFieldData.NumericType {
            return NumericType.DOUBLE
        }

        override fun getFieldName(): String {
            return fieldName
        }

        override fun load(ctx: LeafReaderContext): LeafNumericFieldData {
            return AtomicNumericKeyFieldData(values, keyFieldData.load(ctx))
        }

        override fun loadDirect(ctx: LeafReaderContext): LeafNumericFieldData {
            return load(ctx)
        }
    }
}
