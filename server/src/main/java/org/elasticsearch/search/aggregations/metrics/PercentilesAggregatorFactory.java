/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * This factory is used to generate both TDigest and HDRHisto aggregators, depending
 * on the selected method
 */
class PercentilesAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final PercentilesAggregatorSupplier aggregatorSupplier;
    private final double[] percents;
    private final PercentilesConfig percentilesConfig;
    private final boolean keyed;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            PercentilesAggregationBuilder.REGISTRY_KEY,
            Arrays.asList(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN),
            (name, valuesSource, context, parent, percents, percentilesConfig, keyed,  formatter,
                                          metadata) -> percentilesConfig
                .createPercentilesAggregator(name, valuesSource, context, parent, percents, keyed, formatter, metadata),
                true);
    }

    PercentilesAggregatorFactory(String name, ValuesSourceConfig config, double[] percents,
                                 PercentilesConfig percentilesConfig, boolean keyed, AggregationContext context,
                                 AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
                                 Map<String, Object> metadata,
                                 PercentilesAggregatorSupplier aggregatorSupplier) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
        this.percents = percents;
        this.percentilesConfig = percentilesConfig;
        this.keyed = keyed;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent,
                                        Map<String, Object> metadata) throws IOException {

        return percentilesConfig.createPercentilesAggregator(name, null, context, parent, percents, keyed,
            config.format(), metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return aggregatorSupplier
            .build(name, config.getValuesSource(), context, parent,
                   percents, percentilesConfig, keyed, config.format(), metadata);
    }
}
