/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Main class to swap the index pointed to by an alias, given some conditions
 */
public class TransportRolloverAction extends TransportMasterNodeAction<RolloverRequest, RolloverResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRolloverAction.class);
    private final MetadataRolloverService rolloverService;
    private final ActiveShardsObserver activeShardsObserver;
    private final Client client;

    @Inject
    public TransportRolloverAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                   MetadataRolloverService rolloverService, Client client) {
        super(RolloverAction.NAME, transportService, clusterService, threadPool, actionFilters, RolloverRequest::new,
            indexNameExpressionResolver, RolloverResponse::new, ThreadPool.Names.SAME);
        this.rolloverService = rolloverService;
        this.client = client;
        this.activeShardsObserver = new ActiveShardsObserver(clusterService, threadPool);
    }

    @Override
    protected ClusterBlockException checkBlock(RolloverRequest request, ClusterState state) {
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true,
            request.indicesOptions().expandWildcardsOpen(), request.indicesOptions().expandWildcardsClosed());

        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request));
    }

    @Override
    protected void masterOperation(RolloverRequest request, ClusterState state,
                                   ActionListener<RolloverResponse> listener) throws Exception {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void masterOperation(Task task, final RolloverRequest rolloverRequest, final ClusterState oldState,
                                   final ActionListener<RolloverResponse> listener) throws Exception {

        Metadata metadata = oldState.metadata();

        IndicesStatsRequest statsRequest = new IndicesStatsRequest().indices(rolloverRequest.getRolloverTarget())
            .clear()
            .indicesOptions(IndicesOptions.fromOptions(true, false, true, true))
            .docs(true);
        statsRequest.setParentTask(clusterService.localNode().getId(), task.getId());
        // Rollover can sometimes happen concurrently, to handle these cases, we treat rollover in the same way we would treat a
        // "synchronized" block, in that we have a "before" world, where we calculate naming and condition matching, we then enter our
        // synchronization (in this case, the submitStateUpdateTask which is serialized on the master node), where we then regenerate the
        // names and re-check conditions. More explanation follows inline below.
        //回滚（rollover）有时可以同时发生，为了处理这些情况，我们以与处理“synchronized”块相同的方式处理翻转，因为我们有一个“before”world，我们计算命名和条件匹配，\然后进入我们的同步（在这种情况下，在主节点上序列化的
        // submitStateUpdateTask），然后我们重新生成名称并重新检查条件。更多解释如下。
        client.execute(IndicesStatsAction.INSTANCE, statsRequest,

            ActionListener.wrap(statsResponse -> {
                // Now that we have the stats for the cluster, we need to know the
                // names of the index for which we should evaluate
                // conditions, as well as what our newly created index *would* be.
                //现在我们有了集群的统计信息，我们需要知道我们应该评估条件的索引的名称，以及我们新创建的索引是什么。
                final MetadataRolloverService.NameResolution trialRolloverNames =
                    rolloverService.resolveRolloverNames(oldState, rolloverRequest.getRolloverTarget(),
                        rolloverRequest.getNewIndexName(), rolloverRequest.getCreateIndexRequest());
                final String trialSourceIndexName = trialRolloverNames.sourceName;
                final String trialRolloverIndexName = trialRolloverNames.rolloverName;

                rolloverService.validateIndexName(oldState, trialRolloverIndexName);

                // Evaluate the conditions, so that we can tell without a cluster state update whether a rollover would occur.
                //评估条件，这样我们就可以在没有集群状态更新的情况下判断是否会发生翻转。
                final Map<String, Boolean> trialConditionResults = evaluateConditions(rolloverRequest.getConditions().values(),
                    buildStats(metadata.index(trialSourceIndexName), statsResponse));

                // If this is a dry run, return with the results without invoking a cluster state update
                //如果这是一次试运行，则返回结果而不调用集群状态更新
                if (rolloverRequest.isDryRun()) {
                    listener.onResponse(new RolloverResponse(trialSourceIndexName, trialRolloverIndexName,
                        trialConditionResults, true, false, false, false));
                    return;
                }

                // Holders for what our final source and rolled over index names are as well as the
                // conditions met to cause the rollover, these are needed so we wait on and report
                // the correct indices and conditions in the clusterStateProcessed method
                //我们的最终来源和回滚（rollover）索引名称的持有者以及导致回滚（rollover）的条件，这些都是必需的，因此我们等待并在 clusterStateProcessed 方法中报告正确的索引和条件
                final SetOnce<String> sourceIndex = new SetOnce<>();
                final SetOnce<String> rolloverIndex = new SetOnce<>();
                final SetOnce<Map<String, Boolean>> conditionResults = new SetOnce<>();

                final List<Condition<?>> trialMetConditions = rolloverRequest.getConditions().values().stream()
                    .filter(condition -> trialConditionResults.get(condition.toString())).collect(Collectors.toList());

                // Pre-check the conditions to see whether we should submit a new cluster state task
                //预检查条件，看看我们是否应该提交一个新的集群状态任务
                if (trialConditionResults.size() == 0 || trialMetConditions.size() > 0) {

                    // Submit the cluster state, this can be thought of as a "synchronized"
                    // block in that it is single-threaded on the master node
                    //提交集群状态，这可以被认为是一个“synchronized”块，因为它在主节点上是单线程的
                    clusterService.submitStateUpdateTask("rollover_index source [" + trialRolloverIndexName + "] to target ["
                        + trialRolloverIndexName + "]", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            // Regenerate the rollover names, as a rollover could have happened
                            // in between the pre-check and the cluster state update
                            //重新生成rollover名称，因为在预检查和集群状态更新之间可能发生rollover(回滚)
                            final MetadataRolloverService.NameResolution rolloverNames =
                                rolloverService.resolveRolloverNames(currentState, rolloverRequest.getRolloverTarget(),
                                    rolloverRequest.getNewIndexName(), rolloverRequest.getCreateIndexRequest());
                            final String sourceIndexName = rolloverNames.sourceName;

                            // Re-evaluate the conditions, now with our final source index name
                            //重新评估条件，现在使用我们的最终源索引名称
                            final Map<String, Boolean> postConditionResults = evaluateConditions(rolloverRequest.getConditions().values(),
                                buildStats(metadata.index(sourceIndexName), statsResponse));
                            final List<Condition<?>> metConditions = rolloverRequest.getConditions().values().stream()
                                .filter(condition -> postConditionResults.get(condition.toString())).collect(Collectors.toList());
                            // Update the final condition results so they can be used when returning the response
                            //更新最终条件结果，以便在返回响应时可以使用它们
                            conditionResults.set(postConditionResults);

                            if (postConditionResults.size() == 0 || metConditions.size() > 0) {
                                // Perform the actual rollover 执行实际的回滚（rollover）
                                MetadataRolloverService.RolloverResult rolloverResult =
                                    rolloverService.rolloverClusterState(currentState, rolloverRequest.getRolloverTarget(),
                                        rolloverRequest.getNewIndexName(),
                                        rolloverRequest.getCreateIndexRequest(), metConditions, false, false);
                                logger.trace("rollover result [{}]", rolloverResult);

                                // Update the "final" source and resulting rollover index names.
                                // Note that we use the actual rollover result for these, because
                                // even though we're single threaded, it's possible for the
                                // rollover names generated before the actual rollover to be
                                // different due to things like date resolution
                                //更新“最终”源和生成的rollover索引名称。请注意，我们对这些使用实际的rollover结果，因为即使我们是单线程的，在实际翻转之前生成的翻转名称也可能由于日期解析等原因而有所不同
                                sourceIndex.set(rolloverResult.sourceIndexName);
                                rolloverIndex.set(rolloverResult.rolloverIndexName);

                                // Return the new rollover cluster state, which includes the changes that create the new index
                                //返回新的翻转集群状态，其中包括创建新索引的更改
                                return rolloverResult.clusterState;
                            } else {
                                // Upon re-evaluation of the conditions, none were met, so
                                // therefore do not perform a rollover, returning the current
                                // cluster state.
                                //重新评估条件后，未满足任何条件，因此不要执行rollover，返回当前集群状态。
                                return currentState;
                            }
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            listener.onFailure(e);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            // Now assuming we have a new state and the name of the rolled over index, we need to wait for the
                            // configured number of active shards, as well as return the names of the indices that were rolled/created
                            //现在假设我们有一个新状态和滚动索引的名称，我们需要等待配置的活动分片数量，并返回滚动创建的索引的名称
                            if (newState.equals(oldState) == false) {
                                assert sourceIndex.get() != null : "source index missing on successful rollover";
                                assert rolloverIndex.get() != null : "rollover index missing on successful rollover";
                                assert conditionResults.get() != null : "matching rollover conditions missing on successful rollover";

                                activeShardsObserver.waitForActiveShards(new String[]{rolloverIndex.get()},
                                    rolloverRequest.getCreateIndexRequest().waitForActiveShards(),
                                    rolloverRequest.masterNodeTimeout(),
                                    isShardsAcknowledged -> listener.onResponse(new RolloverResponse(
                                        sourceIndex.get(), rolloverIndex.get(), conditionResults.get(), false, true, true,
                                        isShardsAcknowledged)),
                                    listener::onFailure);
                            } else {
                                // We did not roll over due to conditions not being met inside the cluster state update
                                //由于集群状态更新中未满足条件，我们没有rollover
                                listener.onResponse(new RolloverResponse(
                                    trialSourceIndexName, trialRolloverIndexName, trialConditionResults, false, false, false, false));
                            }
                        }
                    });
                } else {
                    // conditions not met
                    //不满足条件
                    listener.onResponse(new RolloverResponse(trialSourceIndexName, trialRolloverIndexName,
                        trialConditionResults, false, false, false, false));
                }
            }, listener::onFailure));
    }

    static Map<String, Boolean> evaluateConditions(final Collection<Condition<?>> conditions,
                                                   @Nullable final Condition.Stats stats) {
        Objects.requireNonNull(conditions, "conditions must not be null");

        if (stats != null) {
            return conditions.stream()
                .map(condition -> condition.evaluate(stats))
                .collect(Collectors.toMap(result -> result.condition.toString(), result -> result.matched));
        } else {
            // no conditions matched
            //没有匹配的条件
            return conditions.stream()
                .collect(Collectors.toMap(Condition::toString, cond -> false));
        }
    }

    static Condition.Stats buildStats(@Nullable final IndexMetadata metadata,
                                      @Nullable final IndicesStatsResponse statsResponse) {
        if (metadata == null) {
            return null;
        } else {
            final Optional<IndexStats> indexStats = Optional.ofNullable(statsResponse)
                .map(stats -> stats.getIndex(metadata.getIndex().getName()));

            final DocsStats docsStats = indexStats
                .map(stats -> stats.getPrimaries().getDocs())
                .orElse(null);

            final long maxPrimaryShardSize = optionalStream(indexStats)
                .map(IndexStats::getShards)
                .filter(Objects::nonNull)
                .flatMap(Arrays::stream)
                .filter(shard -> shard.getShardRouting().primary())
                .map(ShardStats::getStats)
                .mapToLong(shard -> shard.docs.getTotalSizeInBytes())
                .max().orElse(0);

            return new Condition.Stats(
                docsStats == null ? 0 : docsStats.getCount(),
                metadata.getCreationDate(),
                new ByteSizeValue(docsStats == null ? 0 : docsStats.getTotalSizeInBytes()),
                new ByteSizeValue(maxPrimaryShardSize)
            );
        }
    }

    // helper because java 8 doesn't have Optional.stream
    private static <T> Stream<T> optionalStream(Optional<T> o) {
        if (o.isPresent()) {
            return Stream.of(o.get());
        } else {
            return Stream.empty();
        }
    }
}
