# Lance Rust 测试用例完整列表

## 概览统计

| 维度 | 分类 | 数量 |
|------|------|------|
| **总计** | - | **1887** |

### 按模块分布

| 模块 | 测试数量 | 占比 |
|------|----------|------|
| lance | 706 | 37.4% |
| lance-encoding | 264 | 14.0% |
| lance-index | 187 | 9.9% |
| lance-namespace-impls | 164 | 8.7% |
| lance-core | 140 | 7.4% |
| lance-io | 97 | 5.1% |
| lance-table | 75 | 4.0% |
| lance-arrow | 64 | 3.4% |
| lance-file | 56 | 3.0% |
| lance-datafusion | 53 | 2.8% |
| lance-linalg | 44 | 2.3% |
| lance-namespace | 18 | 1.0% |
| lance-datagen | 11 | 0.6% |
| compression | 5 | 0.3% |
| lance-tools | 3 | 0.2% |

### 按测试对象分布

| 对象 | 测试数量 | 占比 |
|------|----------|------|
| other | 893 | 47.3% |
| index | 185 | 9.8% |
| schema | 137 | 7.3% |
| io | 121 | 6.4% |
| encoding | 94 | 5.0% |
| rowid | 67 | 3.6% |
| fragment | 64 | 3.4% |
| scan | 54 | 2.9% |
| manifest | 53 | 2.8% |
| transaction | 49 | 2.6% |
| append | 44 | 2.3% |
| update | 43 | 2.3% |
| delete | 31 | 1.6% |
| arrow | 22 | 1.2% |
| statistics | 19 | 1.0% |
| compaction | 11 | 0.6% |

### 按复杂度分布

| 复杂度 | 测试数量 | 占比 |
|--------|----------|------|
| simple | 156 | 8.3% |
| medium | 1604 | 85.0% |
| complex | 127 | 6.7% |

### 按测试范围分布

| 范围 | 测试数量 | 占比 |
|------|----------|------|
| unit | 1861 | 98.6% |
| integration | 26 | 1.4% |

---

## 详细测试用例列表

### lance (706 tests)

#### other (171)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `concurrent_create` | ...et/tests/dataset_concurrency_store.rs | complex | unit |
| `test_blob_v2_dedicated_threshold_respects_large...` | lance/src/dataset/blob.rs | complex | unit |
| `test_complex_types_to_json` | lance/src/arrow/json.rs | complex | unit |
| `test_disk_full_error` | lance/src/dataset/write.rs | complex | unit |
| `test_estimate_multivector_vectors_per_row_fallb...` | lance/src/index/vector/utils.rs | complex | unit |
| `test_find_complex_branch` | lance/src/dataset/branch_location.rs | complex | unit |
| `test_json_inverted_multimatch_query` | lance/src/dataset/tests/dataset_index.rs | complex | unit |
| `test_maybe_sample_training_data_multivector_inf...` | lance/src/index/vector/utils.rs | complex | unit |
| `test_multi_base_create` | lance/src/dataset/write.rs | complex | unit |
| `test_multi_base_is_dataset_root_flag` | lance/src/dataset/write.rs | complex | unit |
| `test_multi_base_target_by_path_uri` | lance/src/dataset/write.rs | complex | unit |
| `test_multivector_score` | lance/src/io/exec/knn.rs | complex | unit |
| `test_retain_complex_scenario` | lance/src/dataset/transaction.rs | complex | unit |
| `test_shallow_clone_multiple_times` | lance/src/dataset/tests/dataset_io.rs | complex | unit |
| `check_memory_leak` | lance/tests/resource_test/utils.rs | medium | unit |
| `check_test_spawn_alloc` | lance/tests/resource_test/utils.rs | medium | unit |
| `compute_reassign_assign_ops_moves_vectors_to_ne...` | lance/src/index/vector/builder.rs | medium | unit |
| `do_not_cleanup_newer_data` | lance/src/dataset/cleanup.rs | medium | unit |
| `execute_without_context` | lance/src/io/exec/fts.rs | medium | unit |
| `knn_flat_search` | lance/src/io/exec/knn.rs | medium | unit |
| `no_context_take` | lance/src/io/exec/take.rs | medium | unit |
| `test_adjust_probes_rules` | lance/src/io/exec/knn.rs | medium | unit |
| `test_analyze` | lance/src/dataset/sql.rs | medium | unit |
| `test_analyze_plan` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_auto_cleanup_interval_zero` | lance/src/dataset/cleanup.rs | medium | unit |
| `test_auto_infer_lance_tokenizer` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_bfloat16_roundtrip` | lance/src/dataset/tests/dataset_io.rs | medium | integration |
| `test_binary_vectors_default_to_hamming` | lance/src/dataset/scanner.rs | medium | unit |
| `test_binary_vectors_invalid_distance_error` | lance/src/dataset/scanner.rs | medium | unit |
| `test_blob_v2_dedicated_threshold_ignores_non_po...` | lance/src/dataset/blob.rs | medium | unit |
| `test_blob_v2_dedicated_threshold_respects_small...` | lance/src/dataset/blob.rs | medium | unit |
| `test_boolean_query_parts_searched_metrics` | lance/src/io/exec/fts.rs | medium | unit |
| `test_branch` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_branch_contents_serialization` | lance/src/dataset/refs.rs | medium | unit |
| `test_branch_location_on_windows` | lance/src/dataset/branch_location.rs | medium | unit |
| `test_build_distributed_training_metadata_missing` | lance/src/index/vector.rs | medium | unit |
| `test_build_with_date_open_end_uses_latest` | lance/src/dataset/delta.rs | medium | unit |
| `test_build_with_date_window_edges` | lance/src/dataset/delta.rs | medium | unit |
| `test_can_project_distance` | lance/src/dataset/scanner.rs | medium | unit |
| `test_candidate_bin` | lance/src/dataset/optimize.rs | medium | unit |
| `test_capacity_error` | lance/src/blob.rs | medium | unit |
| `test_check_cosine_normalization` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_cleanup_idempotent` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_codepath_options` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_count_plan` | lance/src/dataset/scanner.rs | medium | unit |
| `test_create_dataset` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_create_knn_flat` | lance/src/io/exec/knn.rs | medium | unit |
| `test_data_type_to_json` | lance/src/arrow/json.rs | medium | unit |
| `test_dataset_too_small` | lance/src/io/exec/knn.rs | medium | unit |
| `test_dataset_uri_roundtrips` | lance/src/dataset/tests/dataset_io.rs | medium | integration |
| `test_ddb_open_iops` | lance/src/io/commit/s3_test.rs | medium | unit |
| `test_deep_clone` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_dv_to_ranges` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_env_var_parsing` | lance/src/dataset/scanner.rs | medium | unit |
| `test_err_ref` | lance/src/dataset/refs.rs | medium | unit |
| `test_explain` | lance/src/dataset/sql.rs | medium | unit |
| `test_explain_plan` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_fast_count_rows` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_fast_search_plan` | lance/src/dataset/scanner.rs | medium | unit |
| `test_fewer_than_k_results` | lance/src/io/exec/knn.rs | medium | unit |
| `test_find_branch_from_same_branch` | lance/src/dataset/branch_location.rs | medium | unit |
| `test_find_main_branch` | lance/src/dataset/branch_location.rs | medium | unit |
| `test_find_main_from_branch` | lance/src/dataset/branch_location.rs | medium | unit |
| `test_find_main_from_root` | lance/src/dataset/branch_location.rs | medium | unit |
| `test_fix_v0_8_0_broken_migration` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_flat_knn` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_fts_accented_chars` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_fts_fuzzy_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_fts_limit_offset` | lance/src/dataset/scanner.rs | medium | unit |
| `test_fts_phrase_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_fts_query_udtf` | lance/src/dataset/udtf.rs | medium | unit |
| `test_fts_rank` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_geo_types` | lance/src/dataset/tests/dataset_geo.rs | medium | unit |
| `test_initialize_indices` | lance/src/index.rs | medium | unit |
| `test_invalid_branch_names` | lance/src/dataset/refs.rs | medium | unit |
| `test_joiner_collect` | lance/src/dataset/hash_joiner.rs | medium | unit |
| `test_json_inverted_boolean_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_json_inverted_flat_match_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_json_inverted_fuzziness_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_json_inverted_match_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_json_inverted_phrase_query` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_knn_limit_offset` | lance/src/dataset/scanner.rs | medium | unit |
| `test_knn_metric_mismatch_falls_back_to_flat_search` | lance/src/dataset/scanner.rs | medium | unit |
| `test_knn_nodes` | lance/src/dataset/scanner.rs | medium | unit |
| `test_knn_with_new_data` | lance/src/dataset/scanner.rs | medium | unit |
| `test_late_materialization` | lance/src/dataset/scanner.rs | medium | unit |
| `test_limit` | lance/src/dataset/scanner.rs | medium | unit |
| `test_limit_cancel` | lance/src/dataset/scanner.rs | medium | unit |
| `test_limit_with_ordering_not_pushed_down` | lance/src/dataset/scanner.rs | medium | unit |
| `test_load_indices` | lance/src/index.rs | medium | unit |
| `test_make_hostile` | lance/src/utils/test.rs | medium | unit |
| `test_max_rows_per_group` | lance/src/dataset/write.rs | medium | unit |
| `test_metadata_roundtrip` | lance/src/arrow/json.rs | medium | integration |
| `test_migrate_v1_to_v3` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_missing_ids` | lance/src/dataset/optimize/remapping.rs | medium | unit |
| `test_missing_indices` | lance/src/dataset/optimize/remapping.rs | medium | unit |
| `test_must_set_on_creation` | lance/src/dataset/rowids.rs | medium | unit |
| `test_nested_json_access` | lance/src/dataset/sql.rs | medium | unit |
| `test_no_max_nprobes` | lance/src/io/exec/knn.rs | medium | unit |
| `test_nullable_struct_v2_1_issue_4385` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_ok_ref` | lance/src/dataset/refs.rs | medium | unit |
| `test_open_dataset_not_found` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_open_nonexisting_dataset` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_out_of_range` | lance/src/dataset/fragment.rs | medium | unit |
| `test_out_of_sync_dataset_can_recover` | lance/src/io/commit/external_manifest.rs | medium | unit |
| `test_out_of_sync_dataset_can_recover` | lance/src/io/commit/dynamodb.rs | medium | unit |
| `test_parse_env_var` | lance/src/dataset/scanner.rs | medium | unit |
| `test_parts_searched_metrics` | lance/src/io/exec/fts.rs | medium | unit |
| `test_pass_session` | lance/src/dataset/write/insert.rs | medium | unit |
| `test_path_functions` | lance/src/dataset/refs.rs | medium | unit |
| `test_plans` | lance/src/dataset/scanner.rs | medium | unit |
| `test_project_nested` | lance/src/dataset/scanner.rs | medium | unit |
| `test_project_node` | lance/src/io/exec/projection.rs | medium | unit |
| `test_query_bool` | lance/tests/query/primitives.rs | medium | unit |
| `test_query_date` | lance/tests/query/primitives.rs | medium | unit |
| `test_query_delta_indices` | lance/src/index/append.rs | medium | unit |
| `test_query_float` | lance/tests/query/primitives.rs | medium | unit |
| `test_query_float_special_values` | lance/tests/query/primitives.rs | medium | unit |
| `test_query_integer` | lance/tests/query/primitives.rs | medium | unit |
| `test_query_timestamp` | lance/tests/query/primitives.rs | medium | unit |
| `test_random_ranges` | lance/src/index/vector/utils.rs | medium | unit |
| `test_recheck` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_refine_factor` | lance/src/dataset/scanner.rs | medium | unit |
| `test_refs_from_traits` | lance/src/dataset/refs.rs | medium | unit |
| `test_reject_invalid` | lance/src/dataset/hash_joiner.rs | medium | unit |
| `test_remap_join_on_second_delta` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_replace_dataset` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_replay` | lance/src/io/exec/utils.rs | medium | unit |
| `test_retain_indices_keeps_system_indices` | lance/src/dataset/transaction.rs | medium | unit |
| `test_reuse_session` | lance/src/dataset/write/commit.rs | medium | unit |
| `test_round_robin_target_base_selection` | lance/src/dataset/write.rs | medium | unit |
| `test_shallow_clone_with_hybrid_paths` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_skip_auto_cleanup` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_some_max_nprobes` | lance/src/io/exec/knn.rs | medium | unit |
| `test_source_dedupe_behavior_first_seen` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_spawn_prereq` | lance/src/utils/future.rs | medium | unit |
| `test_spfresh_join_split` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_table_provider` | lance/src/datafusion/dataframe.rs | medium | unit |
| `test_tag` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_tag_contents_serialization` | lance/src/dataset/refs.rs | medium | unit |
| `test_take` | lance/src/dataset/take.rs | medium | unit |
| `test_take_blob_id_not_exist` | lance/src/dataset/blob.rs | medium | unit |
| `test_take_blob_not_blob_col` | lance/src/dataset/blob.rs | medium | unit |
| `test_take_blobs` | lance/src/dataset/blob.rs | medium | unit |
| `test_take_blobs_by_indices` | lance/src/dataset/blob.rs | medium | unit |
| `test_take_order` | lance/src/io/exec/take.rs | medium | unit |
| `test_take_rows` | lance/src/dataset/take.rs | medium | unit |
| `test_take_rows_out_of_bound` | lance/src/dataset/take.rs | medium | unit |
| `test_take_struct` | lance/src/io/exec/take.rs | medium | unit |
| `test_trim_ranges` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_trim_ranges_by_offset` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_truncate_table` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_v0_7_5_migration` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_valid_branch_names` | lance/src/dataset/refs.rs | medium | unit |
| `test_when_matched_fail` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_with_new_children` | lance/src/io/exec/take.rs | medium | unit |
| `test_build_with_date_window_basic` | lance/src/dataset/delta.rs | simple | unit |
| `test_builder_basic` | lance/src/blob.rs | simple | unit |
| `test_create_and_fill_empty_dataset` | lance/src/dataset/tests/dataset_io.rs | simple | unit |
| `test_create_with_empty_iter` | lance/src/dataset/tests/dataset_io.rs | simple | unit |
| `test_empty_result` | lance/src/io/exec/pushdown_scan.rs | simple | unit |
| `test_empty_uri_rejected` | lance/src/blob.rs | simple | unit |
| `test_find_empty_branch` | lance/src/dataset/branch_location.rs | simple | unit |
| `test_find_simple_branch` | lance/src/dataset/branch_location.rs | simple | unit |
| `test_remap_empty` | lance/src/index.rs | simple | unit |
| `test_retain_empty_indices_vec` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_mixed_empty_nonempty_keeps_nonempty` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_mixed_empty_nonempty_vector_keeps_n...` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_multiple_empty_scalar_indices_keeps...` | lance/src/dataset/transaction.rs | simple | unit |
| `test_search_empty` | lance/src/dataset/tests/dataset_index.rs | simple | unit |
| `test_simple_take` | lance/src/io/exec/take.rs | simple | unit |

#### index (126)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_concurrent_compactions_with_defer_index_remap` | lance/src/dataset/optimize.rs | complex | unit |
| `test_concurrent_create_index` | lance/src/io/commit.rs | complex | unit |
| `test_create_index_with_many_invalid_vectors` | lance/src/index/vector/ivf/v2.rs | complex | unit |
| `test_defer_index_remap_multiple_compactions` | lance/src/dataset/optimize.rs | complex | unit |
| `test_fts_index_with_large_string` | lance/src/dataset/tests/dataset_index.rs | complex | unit |
| `test_index_statistics_updated_at_multiple_deltas` | lance/src/index.rs | complex | unit |
| `cleanup_old_index` | lance/src/dataset/cleanup.rs | medium | unit |
| `dont_clean_index_data_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `no_context_scalar_index` | lance/src/io/exec/scalar_index.rs | medium | unit |
| `remap_ivf_pq_index` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_append_index` | lance/src/index/append.rs | medium | unit |
| `test_bitmap_index_statistics_minimal_io_via_dat...` | lance/src/index.rs | medium | unit |
| `test_bloomfilter_deletion_then_index` | lance/src/index/scalar.rs | medium | unit |
| `test_bloomfilter_index_then_deletion` | lance/src/index/scalar.rs | medium | unit |
| `test_build_ivf_flat` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_build_ivf_model_cosine` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_build_ivf_model_l2` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_build_ivf_pq` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_build_ivf_pq_4bit` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_build_ivf_pq_v3` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_build_ivf_sq` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_build_pq_model_cosine` | lance/src/index/vector/pq.rs | medium | unit |
| `test_build_pq_model_insufficient_rows_returns_p...` | lance/src/index/vector/pq.rs | medium | unit |
| `test_build_pq_model_l2` | lance/src/index/vector/pq.rs | medium | unit |
| `test_count_index_rows` | lance/src/index.rs | medium | unit |
| `test_create_bitmap_index` | lance/src/index.rs | medium | unit |
| `test_create_index` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_create_index_nulls` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_index_rebase_against_update_mem_wal...` | lance/src/index/mem_wal.rs | medium | unit |
| `test_create_index_too_small_for_pq` | lance/src/index.rs | medium | unit |
| `test_create_index_vs_update_mem_wal_state_rebase` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_create_int8_index` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_create_ivf_hnsw_flat` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_create_ivf_hnsw_pq` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_ivf_hnsw_pq` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_create_ivf_hnsw_pq_4bit` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_create_ivf_hnsw_sq` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_create_ivf_pq_cosine` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_ivf_pq_dot` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_ivf_pq_f16` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_ivf_pq_f16_with_codebook` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_ivf_pq_with_centroids` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_ivf_pq_with_invalid_num_sub_vectors` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_create_scalar_index` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_default_index_name` | lance/src/index/create.rs | medium | unit |
| `test_default_index_name_with_special_chars` | lance/src/index/create.rs | medium | unit |
| `test_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_disable_index_cache` | lance/src/session.rs | medium | unit |
| `test_drop_index` | lance/src/index.rs | medium | unit |
| `test_filter_no_scalar_index` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_filter_scalar_index` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_fts_index_with_string` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_fts_unindexed_data` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_fts_without_index` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_geo_rtree_index` | lance/src/dataset/tests/dataset_geo.rs | medium | unit |
| `test_geo_sql` | lance/src/dataset/tests/dataset_geo.rs | medium | unit |
| `test_index_created_at_timestamp` | lance/src/index.rs | medium | unit |
| `test_index_lifecycle_nulls` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_index_matches_criteria_scalar_index` | lance/src/index/scalar.rs | medium | unit |
| `test_index_matches_criteria_vector_index` | lance/src/index/scalar.rs | medium | unit |
| `test_index_name_collision_explicit_errors` | lance/src/index/create.rs | medium | unit |
| `test_index_name_collision_with_explicit_name` | lance/src/index/create.rs | medium | unit |
| `test_index_statistics_updated_at` | lance/src/index.rs | medium | unit |
| `test_index_statistics_updated_at_none_when_no_c...` | lance/src/index.rs | medium | unit |
| `test_index_stats` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_index_take_batch_size` | lance/src/dataset/scanner.rs | medium | unit |
| `test_index_take_batch_size` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_index_with_zero_vectors` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_indexed_merge_insert` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_inexact_scalar_index_plans` | lance/src/dataset/scanner.rs | medium | unit |
| `test_initialize_scalar_index_bitmap` | lance/src/index/scalar.rs | medium | unit |
| `test_initialize_scalar_index_btree` | lance/src/index/scalar.rs | medium | unit |
| `test_initialize_scalar_index_inverted` | lance/src/index/scalar.rs | medium | unit |
| `test_initialize_scalar_index_zonemap` | lance/src/index/scalar.rs | medium | unit |
| `test_initialize_vector_index_ivf_flat` | lance/src/index/vector.rs | medium | unit |
| `test_initialize_vector_index_ivf_hnsw_pq` | lance/src/index/vector.rs | medium | unit |
| `test_initialize_vector_index_ivf_hnsw_sq` | lance/src/index/vector.rs | medium | unit |
| `test_initialize_vector_index_ivf_pq` | lance/src/index/vector.rs | medium | unit |
| `test_initialize_vector_index_ivf_sq` | lance/src/index/vector.rs | medium | unit |
| `test_ivf_pq_limit_offset` | lance/src/dataset/scanner.rs | medium | unit |
| `test_ivf_residual_handling` | lance/src/index/vector/fixture_test.rs | medium | unit |
| `test_knn_no_metric_uses_index_metric` | lance/src/dataset/scanner.rs | medium | unit |
| `test_materialize_index_exec` | lance/src/io/exec/scalar_index.rs | medium | unit |
| `test_merge_index_metadata` | lance/src/index/create.rs | medium | unit |
| `test_merge_insert_use_index` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_optimize_ivf_hnsw_sq_delta_indices` | lance/src/index.rs | medium | unit |
| `test_optimize_ivf_pq_up_to_date` | lance/src/index.rs | medium | unit |
| `test_optimize_scalar_index_btree` | lance/src/index/scalar.rs | medium | unit |
| `test_pq_storage_backwards_compat` | lance/src/index/vector/ivf/v2.rs | medium | unit |
| `test_range_no_scalar_index` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_read_bitmap_index_with_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_read_btree_index_with_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_read_inverted_index_with_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_read_ivf_pq_index_v3_with_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_read_label_list_index_with_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_read_ngram_index_with_defer_index_remap` | lance/src/dataset/optimize.rs | medium | unit |
| `test_recreate_index` | lance/src/index.rs | medium | unit |
| `test_remap_index_after_compaction` | lance/src/dataset/optimize.rs | medium | unit |
| `test_retain_different_index_names` | lance/src/dataset/transaction.rs | medium | unit |
| `test_scalar_index_retained_after_delete_all` | lance/src/index.rs | medium | unit |
| `test_scalar_index_retained_after_update` | lance/src/index.rs | medium | unit |
| `test_secondary_index_scans` | lance/src/dataset/scanner.rs | medium | unit |
| `test_shallow_clone_with_index` | lance/src/index.rs | medium | unit |
| `test_sql_contains_tokens` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_sql_count` | lance/src/dataset/sql.rs | medium | unit |
| `test_sql_execute` | lance/src/dataset/sql.rs | medium | unit |
| `test_sql_to_all_null_transform` | .../dataset/schema_evolution/optimize.rs | medium | unit |
| `test_update_mem_wal_state_against_create_index_...` | lance/src/index/mem_wal.rs | medium | unit |
| `test_update_mem_wal_state_vs_create_index_with_...` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_vector_index_extension_roundtrip` | lance/src/session/index_extension.rs | medium | integration |
| `test_zonemap_deletion_then_index` | lance/src/index/scalar.rs | medium | unit |
| `test_zonemap_index_then_deletion` | lance/src/index/scalar.rs | medium | unit |
| `test_create_empty_scalar_index` | lance/src/index.rs | simple | unit |
| `test_create_fts_index_with_empty_strings` | lance/src/dataset/tests/dataset_index.rs | simple | unit |
| `test_create_fts_index_with_empty_table` | lance/src/dataset/tests/dataset_index.rs | simple | unit |
| `test_create_ivf_hnsw_with_empty_partition` | lance/src/index/vector/ivf.rs | simple | unit |
| `test_filter_on_empty_pq_code` | lance/src/index/vector/pq.rs | simple | unit |
| `test_fts_unindexed_data_on_empty_index` | lance/src/dataset/tests/dataset_index.rs | simple | unit |
| `test_index_stats_empty_partition` | lance/src/index/vector/ivf/v2.rs | simple | unit |
| `test_initialize_single_index` | lance/src/index.rs | simple | unit |
| `test_initialize_vector_index_empty_dataset` | lance/src/index/vector.rs | simple | unit |
| `test_retain_effective_empty_bitmap_single_index` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_single_empty_scalar_index` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_single_empty_vector_index` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_single_index_with_none_bitmap` | lance/src/dataset/transaction.rs | simple | unit |
| `test_retain_single_nonempty_index` | lance/src/dataset/transaction.rs | simple | unit |

#### schema (64)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_fts_on_multiple_columns` | lance/src/dataset/tests/dataset_index.rs | complex | unit |
| `test_sort_multi_columns` | lance/src/dataset/scanner.rs | complex | unit |
| `test_when_matched_delete_full_schema` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_add_column_all_nulls` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_add_column_all_nulls_legacy` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_add_sub_column_to_list_struct_col` | ...set/tests/dataset_schema_evolution.rs | medium | unit |
| `test_add_sub_column_to_packed_struct_col` | ...set/tests/dataset_schema_evolution.rs | medium | unit |
| `test_add_sub_column_to_struct_col` | ...set/tests/dataset_schema_evolution.rs | medium | unit |
| `test_add_sub_column_to_struct_col_unsupported` | ...set/tests/dataset_schema_evolution.rs | medium | unit |
| `test_append_columns_exprs` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_append_columns_udf` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_append_columns_udf_cache` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_append_new_columns` | lance/src/dataset/fragment.rs | medium | unit |
| `test_bad_field_name` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_bitmap_index_on_nested_field_with_dots` | lance/src/index.rs | medium | unit |
| `test_btree_index_on_nested_field_with_dots` | lance/src/index.rs | medium | unit |
| `test_cast_column` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_column_casting_function` | lance/src/dataset/scanner.rs | medium | unit |
| `test_column_not_exist` | lance/src/dataset/scanner.rs | medium | unit |
| `test_compact_blob_columns` | lance/src/dataset/optimize.rs | medium | unit |
| `test_dataset_logicalplan_struct_fields` | lance/src/datafusion/logical_plan.rs | medium | unit |
| `test_drop_add_columns` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_drop_columns` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_drop_list_struct_sub_columns` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_drop_list_struct_sub_columns_legacy` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_errors_on_bad_schema` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_exclude_fields` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_field_metadata` | lance/src/blob.rs | medium | unit |
| `test_file_v1_schema_order` | lance/src/dataset/write.rs | medium | unit |
| `test_fix_schema` | lance/src/io/commit.rs | medium | unit |
| `test_fix_v0_10_5_corrupt_schema` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_initialize_indices_with_missing_field` | lance/src/index.rs | medium | unit |
| `test_insert_balanced_subschemas` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_insert_nested_subschemas` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_insert_subschema` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_invalid_schema` | lance/src/io/exec/rowids.rs | medium | unit |
| `test_inverted_index_on_nested_field_with_dots` | lance/src/index.rs | medium | unit |
| `test_list_struct_field_reorder_issue_5702` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_make_schema` | lance/src/utils/test.rs | medium | unit |
| `test_merge_insert_reordered_columns` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_subschema_invalid_type_error` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_with_action_column` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_nested_field_ordering` | lance/src/dataset/scanner.rs | medium | unit |
| `test_new_column_sql_to_all_nulls_transform_opti...` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_new_column_sql_to_all_nulls_transform_opti...` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_project_preserves_field_metadata` | lance/src/io/exec/projection.rs | medium | unit |
| `test_rename_columns` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_resolve_index_column` | lance/src/index.rs | medium | unit |
| `test_resolve_index_column_error_cases` | lance/src/index.rs | medium | unit |
| `test_resolve_index_column_nested_field` | lance/src/index.rs | medium | unit |
| `test_retain_indices_removes_missing_fields` | lance/src/dataset/transaction.rs | medium | unit |
| `test_row_meta_columns` | lance/src/dataset/scanner.rs | medium | unit |
| `test_schema_mismatch_on_append` | lance/src/dataset/write.rs | medium | unit |
| `test_schema_to_json` | lance/src/arrow/json.rs | medium | unit |
| `test_self_dataset_append_schema_different` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_shuffled_columns` | lance/src/dataset/fragment.rs | medium | unit |
| `test_take_schema` | lance/src/io/exec/take.rs | medium | unit |
| `test_update_field_metadata_by_id` | lance/src/dataset/metadata.rs | medium | unit |
| `test_update_field_metadata_by_path` | lance/src/dataset/metadata.rs | medium | unit |
| `test_update_field_metadata_invalid_id` | lance/src/dataset/metadata.rs | medium | unit |
| `test_update_field_metadata_invalid_path` | lance/src/dataset/metadata.rs | medium | unit |
| `test_update_field_metadata_replace` | lance/src/dataset/metadata.rs | medium | unit |
| `test_vector_index_on_nested_field_with_dots` | lance/src/index.rs | medium | unit |
| `test_vector_index_on_simple_nested_field` | lance/src/index.rs | simple | unit |

#### io (50)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bad_concurrent_config_writes` | lance/src/io/commit.rs | complex | unit |
| `test_concurrent_add_bases_with_data_write` | ...et/tests/dataset_concurrency_store.rs | complex | unit |
| `test_concurrent_writers` | lance/src/io/commit/s3_test.rs | complex | unit |
| `test_concurrent_writes` | lance/src/io/commit.rs | complex | unit |
| `test_good_concurrent_config_writes` | lance/src/io/commit.rs | complex | unit |
| `test_multi_base_overwrite` | lance/src/dataset/write.rs | complex | unit |
| `clean_old_delete_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_blob_v2_sidecar_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_preserves_unmanaged_dirs_and_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_recent_blob_v2_sidecar_files_when_verified` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_recent_verified_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_unreferenced_data_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `create_from_file_v2` | lance/src/dataset/fragment.rs | medium | unit |
| `dont_cleanup_in_progress_write` | lance/src/dataset/cleanup.rs | medium | unit |
| `dont_cleanup_recent_unverified_files` | lance/src/dataset/cleanup.rs | medium | unit |
| `overwrite_dataset` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_binary_filename_generation` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_can_create_dataset_with_external_store` | lance/src/io/commit/external_manifest.rs | medium | unit |
| `test_can_create_dataset_with_external_store` | lance/src/io/commit/dynamodb.rs | medium | unit |
| `test_compact_data_files` | lance/src/dataset/optimize.rs | medium | unit |
| `test_data_file_key_from_path` | lance/src/dataset/blob.rs | medium | unit |
| `test_datafile_partial_replacement` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_datafile_replacement` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_datafile_replacement_error` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_dataset_can_onboard_external_store` | lance/src/io/commit/external_manifest.rs | medium | unit |
| `test_dataset_can_onboard_external_store` | lance/src/io/commit/dynamodb.rs | medium | unit |
| `test_explicit_data_file_bases_path_parsing` | lance/src/dataset/write.rs | medium | unit |
| `test_explicit_data_file_bases_writer_generator` | lance/src/dataset/write.rs | medium | unit |
| `test_file_size` | lance/src/dataset/write.rs | medium | unit |
| `test_iops_read_small` | lance/src/dataset/fragment.rs | medium | unit |
| `test_issue_4902_packed_struct_v2_1_read_error` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_local_object_store` | lance/src/dataset/scanner.rs | medium | unit |
| `test_max_rows_per_file` | lance/src/dataset/write.rs | medium | unit |
| `test_merge_insert_execute_reader_preserves_exte...` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_num_small_files` | lance/src/dataset/tests/dataset_index.rs | medium | unit |
| `test_remove_tombstoned_data_files` | lance/src/dataset/transaction.rs | medium | unit |
| `test_restore` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_restore_deletes` | lance/src/dataset/updater.rs | medium | unit |
| `test_row_offset_read` | lance/src/dataset/scanner.rs | medium | unit |
| `test_session_store_registry` | ...dataset/tests/dataset_transactions.rs | medium | unit |
| `test_store` | lance/src/io/commit/dynamodb.rs | medium | unit |
| `test_strict_overwrite` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_write_and_take_blobs_with_blob_array_builder` | lance/src/dataset/blob.rs | medium | unit |
| `test_write_batch_size` | lance/src/dataset/fragment.rs | medium | unit |
| `test_write_interruption_recovery` | lance/src/dataset/write.rs | medium | unit |
| `test_write_params` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_write_params_validation` | lance/src/dataset/write.rs | medium | unit |
| `test_writer_with_base_id` | lance/src/dataset/write.rs | medium | unit |
| `test_empty_stream_write` | lance/src/dataset/write.rs | simple | unit |
| `test_write_empty_struct` | lance/src/dataset/write/insert.rs | simple | unit |

#### transaction (48)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_concurrent_add_bases_conflict` | ...et/tests/dataset_concurrency_store.rs | complex | unit |
| `test_concurrent_add_bases_name_conflict` | ...et/tests/dataset_concurrency_store.rs | complex | unit |
| `test_concurrent_add_bases_path_conflict` | ...et/tests/dataset_concurrency_store.rs | complex | unit |
| `test_concurrent_commits_are_okay` | lance/src/io/commit/external_manifest.rs | complex | unit |
| `test_concurrent_commits_are_okay` | lance/src/io/commit/dynamodb.rs | complex | unit |
| `test_concurrent_compaction_reindex_compaction_c...` | lance/src/dataset/optimize.rs | complex | unit |
| `test_concurrent_compaction_reindex_reindex_comm...` | lance/src/dataset/optimize.rs | complex | unit |
| `test_inserted_rows_filter_bloom_conflict_detect...` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_list_multiple_transactions` | lance/src/dataset/delta.rs | complex | unit |
| `cleanup_failed_commit_data_file` | lance/src/dataset/cleanup.rs | medium | unit |
| `test_add_bases_id_conflict` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_add_bases_name_conflict` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_add_bases_no_conflict_with_data_operations` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_add_bases_non_conflicting` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_add_bases_path_conflict` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_check_field_conflict` | lance/src/dataset/schema_evolution.rs | medium | unit |
| `test_commit_batch` | lance/src/dataset/write/commit.rs | medium | unit |
| `test_commit_conflict_iops` | lance/src/dataset/write/commit.rs | medium | unit |
| `test_commit_iops` | lance/src/dataset/write/commit.rs | medium | unit |
| `test_conflicting_rebase` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_conflicts` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_conflicts_data_replacement` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_custom_commit` | lance/src/io/commit.rs | medium | unit |
| `test_delete_false_predicate_still_commits` | lance/src/dataset/write/delete.rs | medium | unit |
| `test_delete_true_update_conflict` | lance/src/dataset/write/delete.rs | medium | unit |
| `test_execute_uncommitted` | lance/src/index/create.rs | medium | unit |
| `test_inline_transaction` | ...dataset/tests/dataset_transactions.rs | medium | unit |
| `test_list_contains_deleted_transaction` | lance/src/dataset/delta.rs | medium | unit |
| `test_list_no_transaction` | lance/src/dataset/delta.rs | medium | unit |
| `test_load_and_sort_new_transactions` | lance/src/io/commit.rs | medium | unit |
| `test_merge_insert_conflict_with_append` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_conflict_with_update_without_...` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merged_generations_conflict_equal_generati...` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_merged_generations_conflict_higher_generat...` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_merged_generations_conflict_lower_generati...` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_non_conflicting_rebase_delete_update` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_read_transaction_properties` | ...dataset/tests/dataset_transactions.rs | medium | unit |
| `test_recommit_from_file` | lance/src/dataset/fragment.rs | medium | unit |
| `test_rename_commit_handler` | lance/src/io/commit.rs | medium | unit |
| `test_roundtrip_transaction_file` | lance/src/io/commit.rs | medium | integration |
| `test_transaction_inserted_rows_filter_roundtrip` | lance/src/dataset/write/merge_insert.rs | medium | integration |
| `test_unsafe_commit_handler` | lance/src/io/commit.rs | medium | unit |
| `test_update_mem_wal_state_conflict_equal_genera...` | lance/src/index/mem_wal.rs | medium | unit |
| `test_update_mem_wal_state_conflict_higher_gener...` | lance/src/index/mem_wal.rs | medium | unit |
| `test_update_mem_wal_state_conflict_lower_genera...` | lance/src/index/mem_wal.rs | medium | unit |
| `test_update_mem_wal_state_different_regions_no_...` | lance/src/index/mem_wal.rs | medium | unit |
| `test_v2_manifest_path_commit` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_list_single_transaction` | lance/src/dataset/delta.rs | simple | unit |

#### scan (44)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_filter_on_large_utf8` | lance/src/dataset/scanner.rs | complex | unit |
| `no_context_scan` | lance/src/io/exec/scan.rs | medium | unit |
| `take_scan_dataset` | lance/src/dataset/take.rs | medium | unit |
| `test_ann_prefilter` | lance/src/dataset/scanner.rs | medium | unit |
| `test_at_least_match_filter_scan_range_after_filter` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_bloom_filter_is_not_null_prefilter` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_count_rows_with_filter` | lance/src/dataset/scanner.rs | medium | unit |
| `test_dataset_logicalplan_projection_pd` | lance/src/datafusion/logical_plan.rs | medium | unit |
| `test_dynamic_projection` | lance/src/dataset/scanner.rs | medium | unit |
| `test_edge_cases_limit_pushdown` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_exact_match_filter_scan_range_after_filter` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_filter_parsing` | lance/src/dataset/scanner.rs | medium | unit |
| `test_filter_proj_bug` | lance/src/dataset/scanner.rs | medium | unit |
| `test_filter_to_take` | lance/src/dataset/scanner.rs | medium | unit |
| `test_filter_with_regex` | lance/src/dataset/scanner.rs | medium | unit |
| `test_fts_filter_vector_search` | .../src/dataset/tests/dataset_scanner.rs | medium | unit |
| `test_knn_filter_new_data` | lance/src/dataset/scanner.rs | medium | unit |
| `test_knn_with_filter` | lance/src/dataset/scanner.rs | medium | unit |
| `test_knn_with_prefilter` | lance/src/dataset/scanner.rs | medium | unit |
| `test_limit_pushdown_comprehensive` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_limit_pushdown_in_physical_plan` | ...et/tests/dataset_concurrency_store.rs | medium | unit |
| `test_nested_filter` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_nested_projection` | lance/src/dataset/scanner.rs | medium | unit |
| `test_no_filter_scan_range_before_filter` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_no_prefilter_results` | lance/src/io/exec/knn.rs | medium | unit |
| `test_predicate_combinations` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_projection` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_projection_order` | lance/src/dataset/scanner.rs | medium | unit |
| `test_query_prefilter_date` | lance/tests/query/vectors.rs | medium | unit |
| `test_range_scan_deletions` | lance/src/dataset/fragment.rs | medium | unit |
| `test_scan_blobs` | lance/src/dataset/blob.rs | medium | unit |
| `test_scan_finishes_all_tasks` | lance/src/dataset/scanner.rs | medium | unit |
| `test_scan_limit_offset` | lance/src/dataset/scanner.rs | medium | unit |
| `test_scan_limit_offset_preserves_json_extension...` | .../src/dataset/tests/dataset_scanner.rs | medium | unit |
| `test_scan_order` | lance/src/dataset/scanner.rs | medium | unit |
| `test_scan_planning_io` | lance/src/dataset/scanner.rs | medium | unit |
| `test_scan_sort` | lance/src/dataset/scanner.rs | medium | unit |
| `test_scan_with_wildcard` | lance/src/dataset/scanner.rs | medium | unit |
| `test_take_with_projection` | lance/src/dataset/take.rs | medium | unit |
| `test_vector_filter_fts_search` | .../src/dataset/tests/dataset_scanner.rs | medium | unit |
| `test_with_fetch_limit_pushdown` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_delete_with_single_scanner` | lance/src/dataset/write/delete.rs | simple | unit |
| `test_filter_empty_batches` | lance/src/io/exec/filtered_read.rs | simple | unit |
| `test_scan_regexp_match_and_non_empty_captions` | lance/src/dataset/scanner.rs | simple | unit |

#### rowid (43)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_multiple_merge_insert_stable_row_id` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_stable_row_id_after_multiple_deletion_and_...` | lance/src/dataset/rowids.rs | complex | unit |
| `can_filter_row_id` | lance/src/dataset/scanner.rs | medium | unit |
| `test_assign_row_ids_excess_row_ids` | lance/src/dataset/transaction.rs | medium | unit |
| `test_assign_row_ids_existing_complete` | lance/src/dataset/transaction.rs | medium | unit |
| `test_assign_row_ids_missing_physical_rows` | lance/src/dataset/transaction.rs | medium | unit |
| `test_assign_row_ids_partial_existing` | lance/src/dataset/transaction.rs | medium | unit |
| `test_conditional_update_with_stable_row_id` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_delete_with_row_ids` | lance/src/dataset/rowids.rs | medium | unit |
| `test_deletion_mask_stable_row_id` | lance/src/index/prefilter.rs | medium | unit |
| `test_duplicate_rowid_detection` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_on_row_id` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_merge_state_duplicate_rowid_detection_fail` | ...aset/write/merge_insert/exec/write.rs | medium | unit |
| `test_merge_state_duplicate_rowid_first_seen` | ...aset/write/merge_insert/exec/write.rs | medium | unit |
| `test_new_row_ids` | lance/src/dataset/rowids.rs | medium | unit |
| `test_only_row_id` | lance/src/dataset/scanner.rs | medium | unit |
| `test_retrieve_just_row_id` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_row_id_reader` | lance/src/dataset/fragment.rs | medium | unit |
| `test_row_id_stability_across_update_and_merge_i...` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_row_ids_append` | lance/src/dataset/rowids.rs | medium | unit |
| `test_row_ids_no_nulls` | lance/src/io/exec/rowids.rs | medium | unit |
| `test_row_ids_overwrite` | lance/src/dataset/rowids.rs | medium | unit |
| `test_row_ids_stable_after_update` | lance/src/dataset/write/update.rs | medium | unit |
| `test_row_ids_stable_after_update_odd_id` | lance/src/dataset/write/update.rs | medium | unit |
| `test_row_ids_update` | lance/src/dataset/rowids.rs | medium | unit |
| `test_row_ids_with_nulls` | lance/src/io/exec/rowids.rs | medium | unit |
| `test_rowid_rowaddr_only` | lance/src/dataset/fragment.rs | medium | unit |
| `test_scan_row_ids` | lance/src/dataset/rowids.rs | medium | unit |
| `test_scan_unordered_with_row_id` | lance/src/dataset/scanner.rs | medium | unit |
| `test_stable_row_id_after_deletion_update_and_co...` | lance/src/dataset/rowids.rs | medium | unit |
| `test_stable_row_indices` | lance/src/dataset/optimize.rs | medium | unit |
| `test_take_blobs_by_indices_with_stable_row_ids` | lance/src/dataset/blob.rs | medium | unit |
| `test_take_rowid_rowaddr_with_projection_disable...` | lance/src/dataset/take.rs | medium | unit |
| `test_take_rowid_rowaddr_with_projection_disable...` | lance/src/dataset/take.rs | medium | unit |
| `test_take_rowid_rowaddr_with_projection_enable_...` | lance/src/dataset/take.rs | medium | unit |
| `test_take_rowid_rowaddr_with_projection_enable_...` | lance/src/dataset/take.rs | medium | unit |
| `test_take_rows_with_row_ids` | lance/src/dataset/take.rs | medium | unit |
| `test_update_only_with_stable_row_id` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_upsert_and_delete_all_with_stable_row_id` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_upsert_only_with_stable_row_id` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_upsert_with_conditional_delete_and_stable_...` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_with_row_id` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_empty_dataset_rowids` | lance/src/dataset/rowids.rs | simple | unit |

#### fragment (41)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_assign_row_ids_multiple_fragments` | lance/src/dataset/transaction.rs | complex | unit |
| `test_full_frag_range` | lance/src/io/exec/filtered_read.rs | complex | unit |
| `test_full_schema_upsert_fragment_bitmap` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_upsert_concurrent_full_frag` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_assign_row_ids_new_fragment` | lance/src/dataset/transaction.rs | medium | unit |
| `test_build_distributed_invalid_fragment_ids` | lance/src/index/vector.rs | medium | unit |
| `test_cleanup_frag_reuse_index` | lance/src/dataset/index/frag_reuse.rs | medium | unit |
| `test_fix_v0_21_0_corrupt_fragment_bitmap` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_fragment_count` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_id_never_reset` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_fragment_id_zero_not_reused` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_fragment_scan` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_scan_v2` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_session_take_indices` | lance/src/dataset/fragment/session.rs | medium | unit |
| `test_fragment_session_take_rows` | lance/src/dataset/fragment/session.rs | medium | unit |
| `test_fragment_take_indices` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_take_range_deletions` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_take_rows` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_update` | lance/src/dataset/fragment.rs | medium | unit |
| `test_fragment_write_default_schema` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_fragment_write_validation` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_fragment_write_with_schema` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_make_fragment` | lance/src/utils/test.rs | medium | unit |
| `test_max_fragment_id_migration` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_merge_fragment` | lance/src/dataset/fragment.rs | medium | unit |
| `test_merge_fragments_valid` | lance/src/dataset/transaction.rs | medium | unit |
| `test_metrics_with_limit_partial_fragment` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_replace_fragment_metadata_preserves_fragments` | lance/src/dataset/metadata.rs | medium | unit |
| `test_replace_schema_metadata_preserves_fragments` | lance/src/dataset/metadata.rs | medium | unit |
| `test_restore_does_not_decrease_max_fragment_id` | lance/src/io/commit.rs | medium | unit |
| `test_retain_fragment_bitmap_with_nonexistent_fr...` | lance/src/dataset/transaction.rs | medium | unit |
| `test_retain_indices_keeps_fragment_reuse_index` | lance/src/dataset/transaction.rs | medium | unit |
| `test_rewrite_fragments` | lance/src/dataset/transaction.rs | medium | unit |
| `test_sub_schema_upsert_fragment_bitmap` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_update_affects_index_fragment_bitmap` | lance/src/dataset/write/update.rs | medium | unit |
| `test_update_mixed_indexed_unindexed_fragments` | lance/src/dataset/write/update.rs | medium | unit |
| `test_v0_8_14_invalid_index_fragment_bitmap` | ...c/dataset/tests/dataset_migrations.rs | medium | unit |
| `test_write_fragments_default_schema` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_write_fragments_validation` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_write_fragments_with_options` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_build_distributed_empty_fragment_ids` | lance/src/index/vector.rs | simple | unit |

#### update (28)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_large_merge` | ...dataset/tests/dataset_merge_update.rs | complex | unit |
| `test_merge_insert_large_concurrent` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_merge_multiple_indices` | lance/src/index/vector/ivf/io.rs | complex | unit |
| `test_fast_path_conditional_update` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_fast_path_update_only` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_get_updated_rows` | lance/src/dataset/delta.rs | medium | unit |
| `test_merge` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_merge_indices_after_merge_insert` | lance/src/index/append.rs | medium | unit |
| `test_merge_insert_concurrency` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_defaults_to_unenforced_primar...` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_mixed_case_key` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_permissive_nullability` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_requires_on_or_primary_key` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_subcols` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_insert_updates_indices` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_merge_on_row_addr` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_merged_generations_different_regions_ok` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_plan_upsert` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_update_all` | lance/src/dataset/write/update.rs | medium | unit |
| `test_update_concurrency` | lance/src/dataset/write/update.rs | medium | unit |
| `test_update_conditional` | lance/src/dataset/write/update.rs | medium | unit |
| `test_update_config` | lance/src/dataset/metadata.rs | medium | unit |
| `test_update_merged_generations` | lance/src/index/mem_wal.rs | medium | unit |
| `test_update_same_row_concurrency` | lance/src/dataset/write/update.rs | medium | unit |
| `test_update_table_metadata` | lance/src/dataset/metadata.rs | medium | unit |
| `test_update_validation` | lance/src/dataset/write/update.rs | medium | unit |
| `test_basic_merge` | lance/src/dataset/write/merge_insert.rs | simple | unit |
| `test_empty_merged_generations_noop` | lance/src/index/mem_wal.rs | simple | unit |

#### manifest (26)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `auto_cleanup_old_versions` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_and_retain_3_recent_versions` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_around_tagged_old_versions` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_before_ts_and_retain_n_recent_versions` | lance/src/dataset/cleanup.rs | medium | unit |
| `cleanup_error_when_tagged_old_versions` | lance/src/dataset/cleanup.rs | medium | unit |
| `prevent_blob_version_upgrade_on_overwrite` | lance/src/dataset/write/insert.rs | medium | unit |
| `test_file_write_version` | lance/src/dataset/write.rs | medium | unit |
| `test_filter_by_combined_version_columns` | lance/src/dataset/delta.rs | medium | unit |
| `test_filter_by_row_created_at_version` | lance/src/dataset/delta.rs | medium | unit |
| `test_filter_by_row_last_updated_at_version` | lance/src/dataset/delta.rs | medium | unit |
| `test_filter_version_columns_with_other_columns` | lance/src/dataset/delta.rs | medium | unit |
| `test_load_manifest_iops` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_manifest_partially_fits` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_migrate_v2_manifest_paths` | ...dataset/tests/dataset_transactions.rs | medium | unit |
| `test_overwrite_mixed_version` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_row_version_metadata_after_append` | lance/src/dataset/delta.rs | medium | unit |
| `test_row_version_metadata_after_delete` | lance/src/dataset/delta.rs | medium | unit |
| `test_row_version_metadata_after_update` | lance/src/dataset/delta.rs | medium | unit |
| `test_row_version_metadata_combined` | lance/src/dataset/delta.rs | medium | unit |
| `test_scan_with_version_columns` | lance/src/dataset/scanner.rs | medium | unit |
| `test_v2_manifest_path_create` | ...c/dataset/tests/dataset_versioning.rs | medium | unit |
| `test_write_fragments_with_format_version` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_write_manifest` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_write_with_format_version` | lance/src/dataset/fragment/write.rs | medium | unit |
| `test_row_created_at_version_basic` | lance/src/dataset/delta.rs | simple | unit |
| `test_row_last_updated_at_version_basic` | lance/src/dataset/delta.rs | simple | unit |

#### delete (26)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_concurrent_delete_with_retries` | lance/src/dataset/write/delete.rs | complex | unit |
| `test_join_partition_on_delete_multivec` | lance/src/index/vector/ivf/v2.rs | complex | unit |
| `can_recover_delete_failure` | lance/src/dataset/cleanup.rs | medium | unit |
| `select_reassign_candidates_skips_deleted_partition` | lance/src/index/vector/builder.rs | medium | unit |
| `test_ann_with_deletion` | lance/src/dataset/scanner.rs | medium | unit |
| `test_apply_deletions_invalid_row_address` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_cleanup_removes_only_partial_dirs` | lance/src/index/vector/ivf.rs | medium | unit |
| `test_compact_deletions` | lance/src/dataset/optimize.rs | medium | unit |
| `test_delete` | lance/src/dataset/write/delete.rs | medium | unit |
| `test_delete_concurrency` | lance/src/dataset/write/delete.rs | medium | unit |
| `test_delete_not_supported` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_deletion_mask` | lance/src/index/prefilter.rs | medium | unit |
| `test_is_delete_only` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_limit_offset_with_deleted_rows` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_non_overlapping_rebase_delete_update` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_optimize_should_not_removes_delta_indices` | lance/src/index/create.rs | medium | unit |
| `test_retain_all_indices_removed` | lance/src/dataset/transaction.rs | medium | unit |
| `test_take_with_deletion` | lance/src/dataset/take.rs | medium | unit |
| `test_when_matched_delete_id_only` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_when_matched_delete_no_matches` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_when_matched_delete_with_insert` | lance/src/dataset/write/merge_insert.rs | medium | unit |
| `test_with_deleted_rows` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_with_deletions` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_zonemap_with_deletions` | lance/src/index/scalar.rs | medium | unit |
| `test_retain_multiple_empty_vector_indices_remov...` | lance/src/dataset/transaction.rs | simple | unit |
| `test_search_empty_after_delete` | lance/src/dataset/tests/dataset_index.rs | simple | unit |

#### append (18)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_add_bases_multiple_bases` | lance/src/io/commit/conflict_resolver.rs | complex | unit |
| `test_concurrent_insert_same_new_key` | lance/src/dataset/write/merge_insert.rs | complex | unit |
| `test_multi_base_append` | lance/src/dataset/write.rs | complex | unit |
| `test_partition_split_on_append_multivec` | lance/src/index/vector/ivf/v2.rs | complex | unit |
| `append_dataset` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_add_bases` | ...et/tests/dataset_concurrency_store.rs | medium | unit |
| `test_add_bases_with_none_name` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_add_bases_with_zero_id` | lance/src/io/commit/conflict_resolver.rs | medium | unit |
| `test_add_blanks` | lance/src/dataset/updater.rs | medium | unit |
| `test_address_style_ids` | lance/src/io/exec/rowids.rs | medium | unit |
| `test_get_inserted_rows` | lance/src/dataset/delta.rs | medium | unit |
| `test_insert_builder_first_batch_error` | lance/src/dataset/write/insert.rs | medium | unit |
| `test_insert_builder_preserves_external_error` | lance/src/dataset/write/insert.rs | medium | unit |
| `test_insert_memory` | lance/tests/resource_test/write.rs | medium | unit |
| `test_insert_skip_auto_cleanup` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_load_training_data_addr_sort` | lance/src/index/scalar.rs | medium | unit |
| `test_self_dataset_append` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_take_no_row_addr` | lance/src/io/exec/take.rs | medium | unit |

#### compaction (10)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_compact_many` | lance/src/dataset/optimize.rs | complex | unit |
| `test_concurrent_cleanup_and_compaction_rebase_c...` | lance/src/dataset/optimize.rs | complex | unit |
| `test_concurrent_cleanup_and_compaction_rebase_c...` | lance/src/dataset/optimize.rs | complex | unit |
| `test_compact_all_good` | lance/src/dataset/optimize.rs | medium | unit |
| `test_compact_distributed` | lance/src/dataset/optimize.rs | medium | unit |
| `test_default_compaction_planner` | lance/src/dataset/optimize.rs | medium | unit |
| `test_optimize_delta_indices` | lance/src/index.rs | medium | unit |
| `test_optimize_fts` | lance/src/index.rs | medium | unit |
| `test_compact_empty` | lance/src/dataset/optimize.rs | simple | unit |
| `test_optimize_with_empty_partition` | lance/src/index/vector/ivf/v2.rs | simple | unit |

#### arrow (6)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_chunking_large_batches` | lance/src/dataset/write.rs | complex | unit |
| `test_batch_size` | lance/src/dataset/scanner.rs | medium | unit |
| `test_batch_size` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_chunking_small_batches` | lance/src/dataset/write.rs | medium | unit |
| `test_null_batch` | lance/src/io/exec/pushdown_scan.rs | medium | unit |
| `test_strict_batch_size` | lance/src/dataset/scanner.rs | medium | unit |

#### encoding (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `append_dictionary` | lance/src/dataset/tests/dataset_io.rs | medium | unit |
| `test_issue_4429_nested_struct_encoding_v2_1_wit...` | ...dataset/tests/dataset_merge_update.rs | medium | unit |
| `test_read_struct_of_dictionary_arrays` | lance/src/dataset/tests/dataset_index.rs | medium | unit |

#### statistics (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_statistics` | lance/src/io/exec/filtered_read.rs | medium | unit |
| `test_stats` | lance/src/io/exec/rowids.rs | medium | unit |

### lance-table (75 tests)

#### other (26)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bitmap_iter_full` | lance-table/src/rowids/bitmap.rs | complex | unit |
| `test_base_paths_feature_flags` | lance-table/src/feature_flags.rs | medium | unit |
| `test_bitmap` | lance-table/src/rowids/bitmap.rs | medium | unit |
| `test_bitmap_iter_edge_cases` | lance-table/src/rowids/bitmap.rs | medium | unit |
| `test_bitmap_iter_partial` | lance-table/src/rowids/bitmap.rs | medium | unit |
| `test_bitmap_iter_property` | lance-table/src/rowids/bitmap.rs | medium | unit |
| `test_bitmap_slice` | lance-table/src/rowids/bitmap.rs | medium | unit |
| `test_config` | lance-table/src/format/manifest.rs | medium | unit |
| `test_contains` | lance-table/src/rowids/segment.rs | medium | unit |
| `test_double_ended_iter` | lance-table/src/rowids/encoded_array.rs | medium | unit |
| `test_equality` | lance-table/src/rowids/bitmap.rs | medium | unit |
| `test_mask_to_offset_ranges` | lance-table/src/rowids.rs | medium | unit |
| `test_non_overlapping_ranges` | lance-table/src/rowids/index.rs | medium | unit |
| `test_older_than_with_prerelease` | lance-table/src/format/manifest.rs | medium | unit |
| `test_roundtrip_array` | lance-table/src/io/deletion.rs | medium | integration |
| `test_roundtrip_bitmap` | lance-table/src/io/deletion.rs | medium | integration |
| `test_segments` | lance-table/src/rowids/segment.rs | medium | unit |
| `test_selection` | lance-table/src/rowids.rs | medium | unit |
| `test_selection_unsorted` | lance-table/src/rowids.rs | medium | unit |
| `test_serialization_round_trip` | lance-table/src/rowids/version.rs | medium | unit |
| `test_to_json` | lance-table/src/format/fragment.rs | medium | unit |
| `test_with_new_high` | lance-table/src/rowids/segment.rs | medium | unit |
| `test_with_new_high_assertion` | lance-table/src/rowids/segment.rs | medium | unit |
| `test_with_new_high_assertion_equal` | lance-table/src/rowids/segment.rs | medium | unit |
| `test_basic_zip` | lance-table/src/utils/stream.rs | simple | unit |
| `test_bitmap_iter_empty` | lance-table/src/rowids/bitmap.rs | simple | unit |

#### rowid (13)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_new_index_unsorted_row_ids` | lance-table/src/rowids/index.rs | medium | unit |
| `test_row_id` | lance-table/src/utils/stream.rs | medium | unit |
| `test_row_id_sequence_delete` | lance-table/src/rowids.rs | medium | unit |
| `test_row_id_sequence_extend` | lance-table/src/rowids.rs | medium | unit |
| `test_row_id_sequence_from_range` | lance-table/src/rowids.rs | medium | unit |
| `test_row_id_sequence_rechunk` | lance-table/src/rowids.rs | medium | unit |
| `test_row_id_sequence_to_treemap` | lance-table/src/rowids.rs | medium | unit |
| `test_row_id_slice` | lance-table/src/rowids.rs | medium | unit |
| `test_select_row_ids` | lance-table/src/rowids.rs | medium | unit |
| `test_select_row_ids_out_of_bounds` | lance-table/src/rowids.rs | medium | unit |
| `test_write_read_row_ids` | lance-table/src/rowids/serde.rs | medium | unit |
| `test_row_id_sequence_rechunk_with_empty_segments` | lance-table/src/rowids.rs | simple | unit |
| `test_row_id_slice_empty` | lance-table/src/rowids.rs | simple | unit |

#### manifest (13)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_read_large_manifest` | lance-table/src/io/manifest.rs | complex | unit |
| `test_current_manifest_path` | lance-table/src/io/commit.rs | medium | unit |
| `test_get_version_for_row_id` | lance-table/src/rowids/version.rs | medium | unit |
| `test_list_manifests_sorted` | lance-table/src/io/commit.rs | medium | unit |
| `test_manifest_naming_migration` | lance-table/src/io/commit.rs | medium | unit |
| `test_manifest_naming_scheme` | lance-table/src/io/commit.rs | medium | unit |
| `test_manifest_summary` | lance-table/src/format/manifest.rs | medium | unit |
| `test_version_random_access` | lance-table/src/rowids/version.rs | medium | unit |
| `test_writer_version` | lance-table/src/format/manifest.rs | medium | unit |
| `test_writer_version_comparison_with_prerelease` | lance-table/src/format/manifest.rs | medium | unit |
| `test_writer_version_non_semver` | lance-table/src/format/manifest.rs | medium | unit |
| `test_writer_version_split` | lance-table/src/format/manifest.rs | medium | unit |
| `test_writer_version_with_build_metadata` | lance-table/src/format/manifest.rs | medium | unit |

#### index (6)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_index_with_deletion_vector` | lance-table/src/rowids/index.rs | medium | unit |
| `test_new_index` | lance-table/src/rowids/index.rs | medium | unit |
| `test_new_index_overlap` | lance-table/src/rowids/index.rs | medium | unit |
| `test_new_index_partial_overlap` | lance-table/src/rowids/index.rs | medium | unit |
| `test_new_index_robustness` | lance-table/src/rowids/index.rs | medium | unit |
| `test_completely_empty_index` | lance-table/src/rowids/index.rs | simple | unit |

#### io (5)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_read_check` | lance-table/src/feature_flags.rs | medium | unit |
| `test_write_array` | lance-table/src/io/deletion.rs | medium | unit |
| `test_write_bitmap` | lance-table/src/io/deletion.rs | medium | unit |
| `test_write_check` | lance-table/src/feature_flags.rs | medium | unit |
| `test_write_no_deletions` | lance-table/src/io/deletion.rs | medium | unit |

#### fragment (4)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_fragments_by_offset_range` | lance-table/src/format/manifest.rs | medium | unit |
| `test_new_fragment` | lance-table/src/format/fragment.rs | medium | unit |
| `test_roundtrip_fragment` | lance-table/src/format/fragment.rs | medium | integration |
| `test_empty_fragment_sequences` | lance-table/src/rowids/index.rs | simple | unit |

#### schema (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `data_file_validate_allows_extra_columns` | lance-table/src/format/fragment.rs | medium | unit |
| `test_max_field_id` | lance-table/src/format/manifest.rs | medium | unit |
| `test_update_schema_metadata` | lance-table/src/io/manifest.rs | medium | unit |

#### append (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_row_addr_mask` | lance-table/src/rowids.rs | medium | unit |
| `test_row_addr_mask_everything` | lance-table/src/rowids.rs | medium | unit |

#### encoding (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_encoded_array_from_range` | lance-table/src/rowids/encoded_array.rs | medium | unit |
| `test_encoded_array_from_vec` | lance-table/src/rowids/encoded_array.rs | medium | unit |

#### delete (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_deletes` | lance-table/src/utils/stream.rs | medium | unit |

### lance-core (140 tests)

#### other (66)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `blob_into_unloaded_selects_v2_layout` | lance-core/src/datatypes/field.rs | medium | unit |
| `map_entries_must_be_struct` | lance-core/src/datatypes/field.rs | medium | unit |
| `map_entries_struct_needs_key_and_value` | lance-core/src/datatypes/field.rs | medium | unit |
| `map_key_must_be_non_nullable` | lance-core/src/datatypes/field.rs | medium | unit |
| `map_keys_sorted_unsupported` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_also_allow` | lance-core/src/utils/mask.rs | medium | unit |
| `test_also_block` | lance-core/src/utils/mask.rs | medium | unit |
| `test_and_with_nulls` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_backoff` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_backoff_reset` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_backoff_with_base` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_backoff_with_max` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_backoff_with_min` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_backoff_with_unit` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_bit_utils` | lance-core/src/utils/bit.rs | medium | unit |
| `test_bitand_assign_owned` | lance-core/src/utils/mask.rs | medium | unit |
| `test_blob_path_formatting` | lance-core/src/utils/blob.rs | medium | unit |
| `test_cache_bytes` | lance-core/src/cache.rs | medium | unit |
| `test_cache_trait_objects` | lance-core/src/cache.rs | medium | unit |
| `test_caller_location_capture` | lance-core/src/error.rs | medium | unit |
| `test_compare` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_deep_size_of` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_deserialize_legacy_format` | lance-core/src/utils/mask.rs | medium | unit |
| `test_exp_linked_list_from` | lance-core/src/container/list.rs | medium | unit |
| `test_exp_linked_list_with_capacity_limit` | lance-core/src/container/list.rs | medium | unit |
| `test_explain_difference` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_explain_difference` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_extend` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_external_error_creation` | lance-core/src/error.rs | medium | unit |
| `test_external_source_method` | lance-core/src/error.rs | medium | unit |
| `test_from_roaring` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_from_sorted_iter` | lance-core/src/utils/mask.rs | medium | unit |
| `test_intersection` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_into_external_method` | lance-core/src/error.rs | medium | unit |
| `test_iter_ids` | lance-core/src/utils/mask.rs | medium | unit |
| `test_lance_error_roundtrip_through_datafusion` | lance-core/src/error.rs | medium | integration |
| `test_log_2_ceil` | lance-core/src/utils/bit.rs | medium | unit |
| `test_logical_and` | lance-core/src/utils/mask.rs | medium | unit |
| `test_logical_or` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_extend` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_extend_other_maps` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_from` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_from_roaring` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_intersect` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_mask` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_offset_dense_then_sparse` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_map_serialization_roundtrip` | lance-core/src/utils/mask.rs | medium | integration |
| `test_map_subassign_rows` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_union` | lance-core/src/utils/mask.rs | medium | unit |
| `test_nested_types` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_not_with_nulls` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullability_comparison` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_ops` | lance-core/src/utils/mask.rs | medium | unit |
| `test_or_with_nulls` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_partial_eq` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_range_cardinality` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_row_selection_bit_and` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_row_selection_bit_or` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_selected_indices` | lance-core/src/utils/mask.rs | medium | unit |
| `test_set_bitmap_equality` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_slot_backoff` | lance-core/src/utils/backoff.rs | medium | unit |
| `test_threshold_promotes_to_bitmap` | lance-core/src/utils/deletion.rs | medium | unit |
| `test_u8_slice_key` | lance-core/src/utils/hash.rs | medium | unit |
| `test_union_all` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_deserialize_legacy_empty_lists` | lance-core/src/utils/mask.rs | simple | unit |
| `test_exp_linked_list_basic` | lance-core/src/container/list.rs | simple | unit |

#### schema (23)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `arrow_field_to_field` | lance-core/src/datatypes/field.rs | medium | unit |
| `projection_from_schema_defaults_to_v1` | lance-core/src/datatypes/schema.rs | medium | unit |
| `struct_field` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_all_fields_nullable` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_exclude_fields` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_field_by_id` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_field_intersection` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_field_path_parsing` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_get_nested_field` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_merge_arrow_schema` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_merge_nested_field` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_merge_schemas_and_assign_field_ids` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_project_by_field_null_type` | lance-core/src/datatypes/field.rs | medium | unit |
| `test_resolve_quoted_fields` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_resolve_with_quoted_fields` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_difference_subschema` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_project_by_ids` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_project_by_schema` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_projection` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_unenforced_primary_key` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_unenforced_primary_key_failures` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_schema_unenforced_primary_key_ordering` | lance-core/src/datatypes/schema.rs | medium | unit |
| `test_struct_field_intersection` | lance-core/src/datatypes/field.rs | medium | unit |

#### append (17)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_row_addr_selection_union_all_with_full` | lance-core/src/utils/mask.rs | complex | unit |
| `test_cache_stats_get_or_insert` | lance-core/src/cache.rs | medium | unit |
| `test_insert_range_excluded_end` | lance-core/src/utils/mask.rs | medium | unit |
| `test_insert_range_unbounded_start` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_insert` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_insert_range` | lance-core/src/utils/mask.rs | medium | unit |
| `test_map_into_addr_iter` | lance-core/src/utils/mask.rs | medium | unit |
| `test_nullable_row_addr_set_bitand_fast_path` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_addr_set_bitor_fast_path` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_addr_set_partial_eq` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_addr_set_selected` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_addr_set_with_nulls` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_row_addr_mask_construction` | lance-core/src/utils/mask.rs | medium | unit |
| `test_row_addr_mask_not` | lance-core/src/utils/mask.rs | medium | unit |
| `test_row_addr_selection_deep_size_of` | lance-core/src/utils/mask.rs | medium | unit |
| `test_row_address` | lance-core/src/utils/address.rs | medium | unit |
| `test_nullable_row_addr_set_len_and_is_empty` | lance-core/src/utils/mask/nullable.rs | simple | unit |

#### rowid (11)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_nullable_row_id_mask_bitand_allow_allow_fa...` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitand_allow_block` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitand_allow_block_fa...` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitand_block_block` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitand_block_block_fa...` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitor_allow_allow_fas...` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitor_allow_block` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitor_allow_block_fas...` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_bitor_block_block_fas...` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_drop_nulls` | lance-core/src/utils/mask/nullable.rs | medium | unit |
| `test_nullable_row_id_mask_not_blocklist` | lance-core/src/utils/mask/nullable.rs | medium | unit |

#### fragment (9)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bitand_assign_full_fragments` | lance-core/src/utils/mask.rs | complex | unit |
| `test_bitor_assign_full_fragment` | lance-core/src/utils/mask.rs | complex | unit |
| `test_from_iter_with_full_fragment` | lance-core/src/utils/mask.rs | complex | unit |
| `test_from_iterator_with_full_fragment` | lance-core/src/utils/mask.rs | complex | unit |
| `test_map_insert_full_fragment_row` | lance-core/src/utils/mask.rs | complex | unit |
| `test_remove_from_full_fragment` | lance-core/src/utils/mask.rs | complex | unit |
| `test_sub_assign_with_full_fragments` | lance-core/src/utils/mask.rs | complex | unit |
| `test_map_subassign_frags` | lance-core/src/utils/mask.rs | medium | unit |
| `test_retain_fragments` | lance-core/src/utils/mask.rs | medium | unit |

#### statistics (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_cache_stats_unsized` | lance-core/src/cache.rs | medium | unit |
| `test_cache_stats_with_prefixes` | lance-core/src/cache.rs | medium | unit |
| `test_cache_stats_basic` | lance-core/src/cache.rs | simple | unit |

#### manifest (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_arrow_external_error_conversion` | lance-core/src/error.rs | medium | unit |
| `test_datafusion_arrow_external_error_conversion` | lance-core/src/error.rs | medium | unit |
| `test_datafusion_external_error_conversion` | lance-core/src/error.rs | medium | unit |

#### arrow (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_external_to_arrow_roundtrip` | lance-core/src/error.rs | medium | integration |
| `test_lance_error_roundtrip_through_arrow` | lance-core/src/error.rs | medium | integration |
| `test_roundtrip_arrow` | lance-core/src/utils/mask.rs | medium | integration |

#### io (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `stress_shared_stream` | lance-core/src/utils/futures.rs | complex | unit |
| `test_shared_stream` | lance-core/src/utils/futures.rs | medium | unit |
| `test_unbounded_shared_stream` | lance-core/src/utils/futures.rs | medium | unit |

#### delete (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_map_remove` | lance-core/src/utils/mask.rs | medium | unit |

#### scan (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_build_predicate` | lance-core/src/utils/deletion.rs | medium | unit |

### lance-index (187 tests)

#### other (102)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `handles_large_capacity` | lance-index/src/scalar/zoned.rs | complex | unit |
| `test_from_json_multi_match` | ...e-index/src/scalar/inverted/parser.rs | complex | unit |
| `test_shuffler_multi_buffer_multi_partition` | lance-index/src/vector/ivf/shuffler.rs | complex | unit |
| `test_shuffler_multiple_partition` | lance-index/src/vector/ivf/shuffler.rs | complex | unit |
| `errors_on_out_of_order_offsets` | lance-index/src/scalar/zoned.rs | medium | unit |
| `handles_zone_capacity_one` | lance-index/src/scalar/zoned.rs | medium | unit |
| `rejects_zero_capacity` | lance-index/src/scalar/zoned.rs | medium | unit |
| `search_zones_collects_row_ranges` | lance-index/src/scalar/zoned.rs | medium | unit |
| `test_and_or_preserve_certainty` | lance-index/src/scalar/expression.rs | medium | unit |
| `test_big_shuffle` | lance-index/src/vector/ivf/shuffler.rs | medium | unit |
| `test_binary_quantization` | lance-index/src/vector/bq.rs | medium | unit |
| `test_bitmap_lazy_loading_and_cache` | lance-index/src/scalar/bitmap.rs | medium | unit |
| `test_bitmap_null_handling_in_queries` | lance-index/src/scalar/bitmap.rs | medium | unit |
| `test_bitmap_prewarm` | lance-index/src/scalar/bitmap.rs | medium | unit |
| `test_bitmap_remap` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_bitmap_with_gaps` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_bitmap_working` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_block_max_scores_capacity_matches_block_count` | lance-index/src/scalar/inverted/index.rs | medium | unit |
| `test_bm25_search_uses_global_idf` | lance-index/src/scalar/inverted/index.rs | medium | unit |
| `test_bool_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_boolean_match_plan_boolean_query` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_boolean_match_plan_match_query` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_boolean_match_plan_rejects_non_match_queries` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_boolean_match_plan_rejects_only_must_not` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_centroids_array_getter` | lance-index/src/vector/ivf/storage.rs | medium | unit |
| `test_compute_membership_and_loss` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_compute_on_transposed_codes` | lance-index/src/vector/pq/distance.rs | medium | unit |
| `test_compute_partitions` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_dist_between` | lance-index/src/vector/pq/storage.rs | medium | unit |
| `test_distance_all` | lance-index/src/vector/pq/storage.rs | medium | unit |
| `test_divide_to_subvectors` | lance-index/src/vector/pq/utils.rs | medium | unit |
| `test_equality` | lance-index/src/scalar/btree/flat.rs | medium | unit |
| `test_expressions` | lance-index/src/scalar/expression.rs | medium | unit |
| `test_extract_partition_id` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_f32_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_f64_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_finite_f16` | lance-index/src/vector/transform.rs | medium | unit |
| `test_finite_f32` | lance-index/src/vector/transform.rs | medium | unit |
| `test_finite_f64` | lance-index/src/vector/transform.rs | medium | unit |
| `test_flatten_json_text` | ...inverted/tokenizer/lance_tokenizer.rs | medium | unit |
| `test_flatten_triplet` | ...inverted/tokenizer/lance_tokenizer.rs | medium | unit |
| `test_float16_underflow_fix` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_from_json_boolean` | ...e-index/src/scalar/inverted/parser.rs | medium | unit |
| `test_from_json_boost` | ...e-index/src/scalar/inverted/parser.rs | medium | unit |
| `test_from_json_match` | ...e-index/src/scalar/inverted/parser.rs | medium | unit |
| `test_from_json_phrase` | ...e-index/src/scalar/inverted/parser.rs | medium | unit |
| `test_fsl_to_tensor` | lance-index/src/vector/utils.rs | medium | unit |
| `test_get_chunks` | lance-index/src/vector/sq/storage.rs | medium | unit |
| `test_hash_bytes` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_hierarchical_kmeans` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_hilbert_sort_same_x` | ...src/scalar/rtree/sort/hilbert_sort.rs | medium | unit |
| `test_householder_qr` | lance-index/src/vector/bq/builder.rs | medium | unit |
| `test_i32_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_i64_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_is_all_finite` | lance-index/src/vector/transform.rs | medium | unit |
| `test_is_in` | lance-index/src/scalar/btree/flat.rs | medium | unit |
| `test_json_extract_with_type_info` | lance-index/src/scalar/json.rs | medium | unit |
| `test_json_tokenizer` | ...inverted/tokenizer/lance_tokenizer.rs | medium | unit |
| `test_l2_distance` | lance-index/src/vector/pq.rs | medium | unit |
| `test_l2_with_nans` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_label_list_null_handling` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_mask_set_quick_check` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_match_query_serde` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_nan_ordering` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_ngram_nulls` | lance-index/src/scalar/ngram.rs | medium | unit |
| `test_normalize_transformer_16` | lance-index/src/vector/transform.rs | medium | unit |
| `test_normalize_transformer_f32` | lance-index/src/vector/transform.rs | medium | unit |
| `test_not_flips_certainty` | lance-index/src/scalar/expression.rs | medium | unit |
| `test_null_handling` | lance-index/src/scalar/btree/flat.rs | medium | unit |
| `test_null_ids` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_num_of_bits_from_ndv_fpp` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_pack_unpack_codes` | lance-index/src/vector/bq/storage.rs | medium | unit |
| `test_page_cache` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_phrase_query_serde` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_posting_builder_remap` | lance-index/src/scalar/inverted/index.rs | medium | unit |
| `test_prewarm` | lance-index/src/scalar/rtree.rs | medium | unit |
| `test_range` | lance-index/src/scalar/btree/flat.rs | medium | unit |
| `test_remap` | lance-index/src/scalar/btree/flat.rs | medium | unit |
| `test_remap_bitmap_with_null` | lance-index/src/scalar/bitmap.rs | medium | unit |
| `test_remap_to_nothing` | lance-index/src/scalar/btree/flat.rs | medium | unit |
| `test_sbbf_builder` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_sbbf_numeric_types` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_sbbf_string_types` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_scalar_value_size` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_scale_to_u8_with_nan` | lance-index/src/vector/sq.rs | medium | unit |
| `test_search_bbox` | lance-index/src/scalar/rtree.rs | medium | unit |
| `test_search_null` | lance-index/src/scalar/rtree.rs | medium | unit |
| `test_serialization` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_slice_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_str_as_bytes` | ...ex/src/scalar/bloomfilter/as_bytes.rs | medium | unit |
| `test_tokenizer` | lance-index/src/scalar/ngram.rs | medium | unit |
| `test_train_kmode` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_train_l2_kmeans_with_nans` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_train_with_small_dataset` | lance-index/src/vector/kmeans.rs | medium | unit |
| `test_wand` | lance-index/src/scalar/inverted/wand.rs | medium | unit |
| `test_wand_skip_to_next_block` | lance-index/src/scalar/inverted/wand.rs | medium | unit |
| `search_zones_returns_empty_when_no_match` | lance-index/src/scalar/zoned.rs | simple | unit |
| `test_basic_bitmap` | lance-index/src/scalar/lance_format.rs | simple | unit |
| `test_remap_to_empty_posting_list` | lance-index/src/scalar/inverted/index.rs | simple | unit |
| `test_shuffler_multi_buffer_single_partition` | lance-index/src/vector/ivf/shuffler.rs | simple | unit |
| `test_shuffler_single_partition` | lance-index/src/vector/ivf/shuffler.rs | simple | unit |
| `test_train_empty` | lance-index/src/scalar/ngram.rs | simple | unit |

#### index (41)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_complex_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | complex | unit |
| `test_large_data_types_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | complex | unit |
| `btree_entire_null_page` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_big_bitmap_index` | lance-index/src/scalar/bitmap.rs | medium | unit |
| `test_binary_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_btree_null_handling_in_queries` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_btree_remap_big_deletions` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_btree_types` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_btree_update` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_btree_with_gaps` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_build_pq_storage` | lance-index/src/vector/pq/storage.rs | medium | unit |
| `test_f16_pq_to_protobuf` | lance-index/src/vector/pq.rs | medium | unit |
| `test_f16_sq8` | lance-index/src/vector/sq.rs | medium | unit |
| `test_f32_sq8` | lance-index/src/vector/sq.rs | medium | unit |
| `test_f64_sq8` | lance-index/src/vector/sq.rs | medium | unit |
| `test_inverted_index_without_positions_tracks_fr...` | ...-index/src/scalar/inverted/builder.rs | medium | unit |
| `test_ivf_find_rows` | lance-index/src/vector/ivf/storage.rs | medium | unit |
| `test_label_list_index` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_load_v1_format_ivf` | lance-index/src/vector/ivf/storage.rs | medium | unit |
| `test_merge_ivf_pq_codebook_mismatch` | ...rc/vector/distributed/index_merger.rs | medium | unit |
| `test_merge_ivf_pq_success` | ...rc/vector/distributed/index_merger.rs | medium | unit |
| `test_nan_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_nan_zonemap_index` | lance-index/src/scalar/zonemap.rs | medium | unit |
| `test_ngram_index_merge` | lance-index/src/scalar/ngram.rs | medium | unit |
| `test_ngram_index_remap` | lance-index/src/scalar/ngram.rs | medium | unit |
| `test_ngram_index_with_spill` | lance-index/src/scalar/ngram.rs | medium | unit |
| `test_pq_transform` | lance-index/src/vector/pq.rs | medium | unit |
| `test_pq_transform` | lance-index/src/vector/pq/transform.rs | medium | unit |
| `test_range_btree_index_consistency` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_serialize_deserialize_index_details` | lance-index/src/frag_reuse.rs | medium | unit |
| `test_string_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_time_types_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_timestamp_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_timestamp_types_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_update_ranged_index` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_basic_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | simple | unit |
| `test_basic_btree` | lance-index/src/scalar/lance_format.rs | simple | unit |
| `test_basic_ngram_index` | lance-index/src/scalar/ngram.rs | simple | unit |
| `test_empty_bloomfilter_index` | lance-index/src/scalar/bloomfilter.rs | simple | unit |
| `test_empty_zonemap_index` | lance-index/src/scalar/zonemap.rs | simple | unit |
| `test_merge_ivf_flat_success_basic` | ...rc/vector/distributed/index_merger.rs | simple | unit |

#### fragment (10)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `handles_multi_batch_with_fragment_change` | lance-index/src/scalar/zoned.rs | complex | unit |
| `handles_multiple_batches_same_fragment` | lance-index/src/scalar/zoned.rs | complex | unit |
| `rebuild_zones_handles_multi_fragment_stream` | lance-index/src/scalar/zoned.rs | complex | unit |
| `test_multiple_fragments_bloomfilter` | lance-index/src/scalar/bloomfilter.rs | complex | unit |
| `flushes_on_fragment_boundary` | lance-index/src/scalar/zoned.rs | medium | unit |
| `handles_non_contiguous_fragment_ids` | lance-index/src/scalar/zoned.rs | medium | unit |
| `test_fragment_btree_index_boundary_queries` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_fragment_btree_index_consistency` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_fragment_id_assignment` | lance-index/src/scalar/zonemap.rs | medium | unit |
| `splits_single_fragment` | lance-index/src/scalar/zoned.rs | simple | unit |

#### io (7)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_block_max_score_matches_stored_value` | lance-index/src/scalar/inverted/wand.rs | medium | unit |
| `test_builder_write_load` | lance-index/src/vector/hnsw/builder.rs | medium | unit |
| `test_cleanup_partition_files` | lance-index/src/scalar/btree.rs | medium | unit |
| `test_json_text_stream` | lance-index/src/scalar/inverted/json.rs | medium | unit |
| `test_merge_streams_partitions_in_batches` | ...e-index/src/scalar/inverted/merger.rs | medium | unit |
| `test_skip_merge_writes_partitions_as_is` | ...-index/src/scalar/inverted/builder.rs | medium | unit |
| `test_write_and_load` | lance-index/src/vector/ivf/storage.rs | medium | unit |

#### update (7)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bitmap_update` | lance-index/src/scalar/lance_format.rs | medium | unit |
| `test_merge_content_key_order_invariance` | ...rc/vector/distributed/index_merger.rs | medium | unit |
| `test_merge_distance_type_mismatch` | ...rc/vector/distributed/index_merger.rs | medium | unit |
| `test_merge_partial_order_tie_breaker` | ...rc/vector/distributed/index_merger.rs | medium | unit |
| `test_merge_reuses_token_ids_for_shared_tokens` | ...e-index/src/scalar/inverted/merger.rs | medium | unit |
| `test_update_and_search` | lance-index/src/scalar/rtree.rs | medium | unit |
| `test_update_empty` | lance-index/src/scalar/ngram.rs | simple | unit |

#### schema (5)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_boolean_match_plan_rejects_missing_column` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_boolean_match_plan_rejects_mixed_columns` | lance-index/src/scalar/inverted/query.rs | medium | unit |
| `test_drop_column` | lance-index/src/vector/transform.rs | medium | unit |
| `test_normalize_transformer_with_output_column` | lance-index/src/vector/transform.rs | medium | unit |
| `test_remap_with_extra_column` | lance-index/src/vector/pq/storage.rs | medium | unit |

#### append (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `rebuild_zones_appends_new_stats` | lance-index/src/scalar/zoned.rs | medium | unit |
| `test_block_insert_and_check` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |
| `test_sbbf_insert_and_check` | ...-index/src/scalar/bloomfilter/sbbf.rs | medium | unit |

#### encoding (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_compress_positions` | ...index/src/scalar/inverted/encoding.rs | medium | unit |
| `test_compress_posting_list` | ...index/src/scalar/inverted/encoding.rs | medium | unit |
| `test_posting_iterator_next_compressed_partition...` | lance-index/src/scalar/inverted/wand.rs | medium | unit |

#### scan (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bloomfilter_null_handling_in_queries` | lance-index/src/scalar/bloomfilter.rs | medium | unit |
| `test_bloomfilter_supported_operations` | lance-index/src/scalar/bloomfilter.rs | medium | unit |

#### arrow (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `handles_empty_batches` | lance-index/src/scalar/zoned.rs | simple | unit |
| `test_shuffler_single_partition_with_empty_batch` | lance-index/src/vector/ivf/shuffler.rs | simple | unit |

#### delete (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `handles_deletion_with_large_gaps` | lance-index/src/scalar/zoned.rs | complex | unit |
| `handles_non_contiguous_offsets_after_deletion` | lance-index/src/scalar/zoned.rs | medium | unit |

#### statistics (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_zonemap_null_handling_in_queries` | lance-index/src/scalar/zonemap.rs | medium | unit |

#### transaction (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_posting_cache_conflict_across_partitions` | lance-index/src/scalar/inverted/index.rs | medium | unit |

#### compaction (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_build_dist_table_not_optimized` | lance-index/src/vector/bq/storage.rs | medium | unit |

### lance-encoding (264 tests)

#### other (176)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `fsl_value_miniblock_stress` | ...oding/src/encodings/physical/value.rs | complex | unit |
| `fsl_value_per_value_stress` | ...oding/src/encodings/physical/value.rs | complex | unit |
| `test_binary_large_minichunk_size_over_max_minib...` | ...ng/src/encodings/logical/primitive.rs | complex | unit |
| `test_fixed_size_large_binary` | ...codings/physical/fixed_size_binary.rs | complex | unit |
| `test_fixed_size_large_utf8` | ...codings/physical/fixed_size_binary.rs | complex | unit |
| `test_fsst_large_binary` | ...ding/src/encodings/physical/binary.rs | complex | unit |
| `test_fullzip_cache_config_controls_caching` | ...ng/src/encodings/logical/primitive.rs | complex | unit |
| `test_large` | lance-encoding/src/data.rs | complex | unit |
| `test_large_binary` | ...ding/src/encodings/physical/binary.rs | complex | unit |
| `test_large_binary` | ...ious/encodings/physical/dictionary.rs | complex | unit |
| `test_large_list` | ...ncoding/src/encodings/logical/list.rs | complex | unit |
| `test_large_primitive` | ...oding/src/encodings/physical/value.rs | complex | unit |
| `test_large_utf8` | ...ding/src/encodings/physical/binary.rs | complex | unit |
| `test_large_utf8` | ...ious/encodings/physical/dictionary.rs | complex | unit |
| `test_miniblock_stress` | ...oding/src/encodings/physical/value.rs | complex | unit |
| `test_multi_chunk_round_trip` | ...ncoding/src/encodings/physical/rle.rs | complex | unit |
| `test_repdef_multiple_builders` | lance-encoding/src/repdef.rs | complex | unit |
| `variable_packed_struct_large_utf8_round_trip` | ...ding/src/encodings/physical/packed.rs | complex | unit |
| `variable_packed_struct_multi_variable_round_trip` | ...ding/src/encodings/physical/packed.rs | complex | unit |
| `regress_list_ends_null_case` | lance-encoding/src/repdef.rs | medium | unit |
| `regress_list_fsl` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_1024_boundary_conditions` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_all_null` | lance-encoding/src/data.rs | medium | unit |
| `test_all_valid_combinations` | ...-encoding/src/encodings/fuzz_tests.rs | medium | unit |
| `test_bigger_than_max_page_size` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_binary` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_binary` | ...ious/encodings/physical/dictionary.rs | medium | unit |
| `test_binary_fsst` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_binary_miniblock_with_misaligned_buffer` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_bit_slice_le` | lance-encoding/src/buffer.rs | medium | unit |
| `test_bit_width_stat_for_integers` | lance-encoding/src/statistics.rs | medium | unit |
| `test_bit_width_stat_more_than_1024` | lance-encoding/src/statistics.rs | medium | unit |
| `test_bit_width_when_none` | lance-encoding/src/statistics.rs | medium | unit |
| `test_bitmap_boolean` | ...previous/encodings/physical/bitmap.rs | medium | unit |
| `test_blob_round_trip` | ...ncoding/src/encodings/logical/blob.rs | medium | unit |
| `test_blob_v2_dedicated_round_trip` | ...ncoding/src/encodings/logical/blob.rs | medium | unit |
| `test_blob_v2_external_round_trip` | ...ncoding/src/encodings/logical/blob.rs | medium | unit |
| `test_blob_v2_packed_round_trip` | ...ncoding/src/encodings/logical/blob.rs | medium | unit |
| `test_buffer_consistency` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_cardinality_variable_width_datablock` | lance-encoding/src/statistics.rs | medium | unit |
| `test_coalesce_indices_to_ranges` | lance-encoding/src/decoder.rs | medium | unit |
| `test_coalesce_indices_to_ranges_with_gaps` | lance-encoding/src/decoder.rs | medium | unit |
| `test_complicated_struct` | ...oding/src/encodings/logical/struct.rs | medium | unit |
| `test_composite_unravel` | lance-encoding/src/repdef.rs | medium | unit |
| `test_compute_start_offset` | ...revious/encodings/physical/bitpack.rs | medium | unit |
| `test_concat` | lance-encoding/src/buffer.rs | medium | unit |
| `test_control_words` | lance-encoding/src/repdef.rs | medium | unit |
| `test_data_size` | lance-encoding/src/data.rs | medium | unit |
| `test_data_size_stat` | lance-encoding/src/statistics.rs | medium | unit |
| `test_deeply_nested_lists` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_default_behavior` | lance-encoding/src/compression.rs | medium | unit |
| `test_drain_instructions` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_edge_cases_all_nulls` | ...-encoding/src/encodings/fuzz_tests.rs | medium | unit |
| `test_eq` | lance-encoding/src/buffer.rs | medium | unit |
| `test_exact_match_priority` | lance-encoding/src/compression_config.rs | medium | unit |
| `test_fixed_size_binary` | ...codings/physical/fixed_size_binary.rs | medium | unit |
| `test_fixed_size_sliced_utf8` | ...codings/physical/fixed_size_binary.rs | medium | unit |
| `test_fixed_size_utf8_binary` | ...codings/physical/fixed_size_binary.rs | medium | unit |
| `test_fsl_all_null` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_fsl_bitmap_boolean` | ...previous/encodings/physical/bitmap.rs | medium | unit |
| `test_fsl_list_rejected` | .../encodings/logical/fixed_size_list.rs | medium | unit |
| `test_fsl_map_rejected` | .../encodings/logical/fixed_size_list.rs | medium | unit |
| `test_fsl_nullable_items` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_fsl_struct_random` | .../encodings/logical/fixed_size_list.rs | medium | unit |
| `test_fsst` | ...coding/src/encodings/physical/fsst.rs | medium | unit |
| `test_fuzz_issue_4466` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_hex` | lance-encoding/src/buffer.rs | medium | unit |
| `test_invalid_buffer_count` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_legacy_metadata_support` | lance-encoding/src/compression.rs | medium | unit |
| `test_list` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_list_of_maps` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_list_repdef_handling` | ...-encoding/src/encodings/fuzz_tests.rs | medium | unit |
| `test_list_struct_list` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_list_with_garbage_nulls` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_long_run_splitting` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_low_repetition_50pct_bug` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_map_all_null` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_map_different_key_types` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_map_in_nullable_struct` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_map_in_struct` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_map_range` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_map_with_extreme_sizes` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_map_with_null_values` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_max_length_variable_width_datablock` | lance-encoding/src/statistics.rs | medium | unit |
| `test_minichunk_size_128kb_v2_2` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_minichunk_size_roundtrip` | ...ng/src/encodings/logical/primitive.rs | medium | integration |
| `test_mixed_page_validity` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_mixed_unraveler` | lance-encoding/src/repdef.rs | medium | unit |
| `test_nested_fsl` | ...encodings/physical/fixed_size_list.rs | medium | unit |
| `test_nested_list` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_nested_map` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_nested_strings` | ...ncoding/src/encodings/logical/list.rs | medium | unit |
| `test_nullable_struct` | ...oding/src/encodings/logical/struct.rs | medium | unit |
| `test_only_null_lists` | lance-encoding/src/repdef.rs | medium | unit |
| `test_pattern_matching` | lance-encoding/src/compression.rs | medium | unit |
| `test_pattern_matching` | lance-encoding/src/compression_config.rs | medium | unit |
| `test_power_of_two_chunking` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_ragged_scheduling` | ...oding/src/encodings/logical/struct.rs | medium | unit |
| `test_random_packed_struct` | ...s/encodings/physical/packed_struct.rs | medium | unit |
| `test_reinterpret_vec` | lance-encoding/src/buffer.rs | medium | unit |
| `test_repdef_abnormal_nulls` | lance-encoding/src/repdef.rs | medium | unit |
| `test_repdef_all_valid` | lance-encoding/src/repdef.rs | medium | unit |
| `test_repdef_fsl` | lance-encoding/src/repdef.rs | medium | unit |
| `test_repdef_fsl_allvalid_item` | lance-encoding/src/repdef.rs | medium | unit |
| `test_repdef_no_rep` | lance-encoding/src/repdef.rs | medium | unit |
| `test_repdef_sliced_offsets` | lance-encoding/src/repdef.rs | medium | unit |
| `test_round_trip_all_types` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_round_trip_f32` | ...codings/physical/byte_stream_split.rs | medium | unit |
| `test_round_trip_f64` | ...codings/physical/byte_stream_split.rs | medium | unit |
| `test_rows_in_buffer` | ...revious/encodings/physical/bitpack.rs | medium | unit |
| `test_run_count_stat` | lance-encoding/src/statistics.rs | medium | unit |
| `test_schedule_instructions` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_sliced_to_data_block` | lance-encoding/src/data.rs | medium | unit |
| `test_sliced_utf8` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_sliced_utf8` | ...ious/encodings/physical/dictionary.rs | medium | unit |
| `test_slicer` | lance-encoding/src/repdef.rs | medium | unit |
| `test_small_strings` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_specific_packed_struct` | ...s/encodings/physical/packed_struct.rs | medium | unit |
| `test_string_sliced` | lance-encoding/src/data.rs | medium | unit |
| `test_string_to_data_block` | lance-encoding/src/data.rs | medium | unit |
| `test_struct_list` | ...oding/src/encodings/logical/struct.rs | medium | unit |
| `test_tiny_boolean` | ...previous/encodings/physical/bitmap.rs | medium | unit |
| `test_to_typed_slice` | lance-encoding/src/buffer.rs | medium | unit |
| `test_to_typed_slice_invalid` | lance-encoding/src/buffer.rs | medium | unit |
| `test_type_level_parameters` | lance-encoding/src/compression.rs | medium | unit |
| `test_utf8` | ...ious/encodings/physical/dictionary.rs | medium | unit |
| `test_utf8_binary` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_value_fsl_primitive` | ...encodings/physical/fixed_size_list.rs | medium | unit |
| `test_value_primitive` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_zip` | lance-encoding/src/buffer.rs | medium | unit |
| `variable_packed_struct_requires_v22` | ...ding/src/encodings/physical/packed.rs | medium | unit |
| `variable_packed_struct_round_trip` | ...ding/src/encodings/physical/packed.rs | medium | unit |
| `variable_packed_struct_utf8_round_trip` | ...ding/src/encodings/physical/packed.rs | medium | unit |
| `regress_empty_list_case` | lance-encoding/src/repdef.rs | simple | unit |
| `test_basic_blob` | ...rc/previous/encodings/logical/blob.rs | simple | unit |
| `test_edge_cases_single_value` | ...-encoding/src/encodings/fuzz_tests.rs | simple | unit |
| `test_empty_data` | ...codings/physical/byte_stream_split.rs | simple | unit |
| `test_empty_data_handling` | ...ncoding/src/encodings/physical/rle.rs | simple | unit |
| `test_empty_list_list` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_empty_lists` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_empty_maps` | ...encoding/src/encodings/logical/map.rs | simple | unit |
| `test_empty_strings` | ...ding/src/encodings/physical/binary.rs | simple | unit |
| `test_empty_strings` | ...ious/encodings/physical/dictionary.rs | simple | unit |
| `test_empty_struct` | ...oding/src/encodings/logical/struct.rs | simple | unit |
| `test_fixed_size_empty_strings` | ...codings/physical/fixed_size_binary.rs | simple | unit |
| `test_fuzz_issue_4492_empty_rep_values` | ...ng/src/encodings/logical/primitive.rs | simple | unit |
| `test_list_struct_empty` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_null_and_empty_lists` | lance-encoding/src/repdef.rs | simple | unit |
| `test_only_empty_lists` | lance-encoding/src/repdef.rs | simple | unit |
| `test_repdef_basic` | lance-encoding/src/repdef.rs | simple | unit |
| `test_repdef_complex_null_empty` | lance-encoding/src/repdef.rs | simple | unit |
| `test_repdef_empty_list_at_end` | lance-encoding/src/repdef.rs | simple | unit |
| `test_repdef_empty_list_no_null` | lance-encoding/src/repdef.rs | simple | unit |
| `test_repdef_simple_null_empty_list` | lance-encoding/src/repdef.rs | simple | unit |
| `test_simple_binary` | ...ding/src/encodings/physical/binary.rs | simple | unit |
| `test_simple_blob` | ...rc/previous/encodings/logical/blob.rs | simple | unit |
| `test_simple_boolean` | ...previous/encodings/physical/bitmap.rs | simple | unit |
| `test_simple_fixed_size_utf8` | ...codings/physical/fixed_size_binary.rs | simple | unit |
| `test_simple_fixed_size_with_nulls_utf8` | ...codings/physical/fixed_size_binary.rs | simple | unit |
| `test_simple_fsl` | ...encodings/physical/fixed_size_list.rs | simple | unit |
| `test_simple_large_list` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_list` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_list_all_null` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_map` | ...encoding/src/encodings/logical/map.rs | simple | unit |
| `test_simple_masked_nonempty_list` | ...oding/src/encodings/logical/struct.rs | simple | unit |
| `test_simple_nested_list_ends_with_null` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_range` | ...oding/src/encodings/physical/value.rs | simple | unit |
| `test_simple_sliced_list` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_string_list` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_string_list_no_null` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_struct` | ...oding/src/encodings/logical/struct.rs | simple | unit |
| `test_simple_struct_list` | ...oding/src/encodings/logical/struct.rs | simple | unit |
| `test_simple_two_page_list` | ...ncoding/src/encodings/logical/list.rs | simple | unit |
| `test_simple_utf8` | ...ious/encodings/physical/dictionary.rs | simple | unit |
| `test_simple_value` | ...oding/src/encodings/physical/value.rs | simple | unit |
| `test_simple_wide_fsl` | ...encodings/physical/fixed_size_list.rs | simple | unit |

#### encoding (71)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_compressed_mini_block_large_buffers` | ...ing/src/encodings/physical/general.rs | complex | unit |
| `test_compressed_mini_block_rle_multiple_buffers` | ...ing/src/encodings/physical/general.rs | complex | unit |
| `test_dict_encoding_should_not_be_applied_if_car...` | lance-encoding/src/previous/encoder.rs | complex | unit |
| `test_fixed_size_binary_encoding_applicable_mult...` | lance-encoding/src/previous/encoder.rs | complex | unit |
| `test_large_dictionary_general_compression` | ...ng/src/encodings/logical/primitive.rs | complex | unit |
| `test_zstd_compress_decompress_multiple_times` | ...oding/src/encodings/physical/block.rs | complex | unit |
| `test_binary_dictionary_encoding` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_binary_encoding_verification` | ...ding/src/encodings/physical/binary.rs | medium | unit |
| `test_bitmap_decoder_edge_cases` | ...previous/encodings/physical/bitmap.rs | medium | unit |
| `test_bitpack_encoding_verification` | .../src/encodings/physical/bitpacking.rs | medium | unit |
| `test_bitpack_params` | ...revious/encodings/physical/bitpack.rs | medium | unit |
| `test_bitpack_primitive` | ...revious/encodings/physical/bitpack.rs | medium | unit |
| `test_blob_encoder_creation` | ...ncoding/src/encodings/logical/blob.rs | medium | unit |
| `test_bss_with_compression` | lance-encoding/src/compression.rs | medium | unit |
| `test_bytepacked_integer_encoder` | lance-encoding/src/utils/bytepack.rs | medium | unit |
| `test_choose_encoder_for_zstd_compression_level` | lance-encoding/src/previous/encoder.rs | medium | unit |
| `test_compress_zstd_raw_stream_format_and_decomp...` | ...oding/src/encodings/physical/block.rs | medium | unit |
| `test_compress_zstd_with_length_prefixed` | ...oding/src/encodings/physical/block.rs | medium | unit |
| `test_compressed_buffer_encoder` | .../previous/encodings/physical/block.rs | medium | unit |
| `test_compressed_mini_block_table_driven` | ...ing/src/encodings/physical/general.rs | medium | unit |
| `test_compressed_mini_block_threshold` | ...ing/src/encodings/physical/general.rs | medium | unit |
| `test_compressed_mini_block_with_doubles` | ...ing/src/encodings/physical/general.rs | medium | unit |
| `test_compression_scheme_from_str` | ...oding/src/encodings/physical/block.rs | medium | unit |
| `test_compression_scheme_from_str_invalid` | ...oding/src/encodings/physical/block.rs | medium | unit |
| `test_configured_encoding_strategy` | lance-encoding/src/encoder.rs | medium | unit |
| `test_decimal128_dictionary_encoding` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_dict_encoding_should_be_applied_if_cardina...` | lance-encoding/src/previous/encoder.rs | medium | unit |
| `test_dict_encoding_should_not_be_applied_for_sm...` | lance-encoding/src/previous/encoder.rs | medium | unit |
| `test_dict_encoding_should_not_be_applied_if_car...` | lance-encoding/src/previous/encoder.rs | medium | unit |
| `test_dictionary_cannot_add_null` | lance-encoding/src/data.rs | medium | unit |
| `test_dictionary_cannot_concatenate` | lance-encoding/src/data.rs | medium | unit |
| `test_dictionary_encode_float64` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_dictionary_encode_int64` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_dictionary_indices_normalized` | lance-encoding/src/data.rs | medium | unit |
| `test_dictionary_nulls` | lance-encoding/src/data.rs | medium | unit |
| `test_encode_dict_nulls` | ...ious/encodings/physical/dictionary.rs | medium | unit |
| `test_encode_indices_adjusts_nulls` | ...previous/encodings/physical/binary.rs | medium | unit |
| `test_encoding_round_trip` | ...-encoding/src/encodings/fuzz_tests.rs | medium | unit |
| `test_estimate_dict_size_fixed_width` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_estimate_dict_size_variable_width` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_fixed_size_binary_decoder` | ...codings/physical/fixed_size_binary.rs | medium | unit |
| `test_fixed_size_binary_encoding_applicable` | lance-encoding/src/previous/encoder.rs | medium | unit |
| `test_fixed_size_list_encoding` | ...-encoding/src/encodings/fuzz_tests.rs | medium | unit |
| `test_fsl_value_compression_miniblock` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_fsl_value_compression_per_value` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_general_block_decompression_fixed_width_v2_2` | lance-encoding/src/compression.rs | medium | unit |
| `test_get_buffer_decoder_for_compressed_buffer` | ...ng/src/previous/encodings/physical.rs | medium | unit |
| `test_low_cardinality_prefers_bitpacking_over_rle` | lance-encoding/src/compression.rs | medium | unit |
| `test_lz4_compress_decompress` | ...oding/src/encodings/physical/block.rs | medium | unit |
| `test_lz4_compress_round_trip` | ...oding/src/encodings/physical/block.rs | medium | unit |
| `test_map_encoder_keep_original_array_scenarios` | ...encoding/src/encodings/logical/map.rs | medium | unit |
| `test_miniblock_bitpack` | .../src/encodings/physical/bitpacking.rs | medium | unit |
| `test_miniblock_bitpack_zero_chunk_selection` | .../src/encodings/physical/bitpacking.rs | medium | unit |
| `test_none_compression` | lance-encoding/src/compression.rs | medium | unit |
| `test_num_compressed_bits_signed_types` | ...revious/encodings/physical/bitpack.rs | medium | unit |
| `test_out_of_line_bitpack_raw_tail_roundtrip` | .../src/encodings/physical/bitpacking.rs | medium | integration |
| `test_parameter_based_compression` | lance-encoding/src/compression.rs | medium | unit |
| `test_random_dictionary_input` | ...ious/encodings/physical/dictionary.rs | medium | unit |
| `test_rle_encoding_verification` | ...ncoding/src/encodings/physical/rle.rs | medium | unit |
| `test_rle_with_general_miniblock_wrapper` | ...ing/src/encodings/physical/general.rs | medium | unit |
| `test_should_dictionary_encode` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_should_not_dictionary_encode` | ...ng/src/encodings/logical/primitive.rs | medium | unit |
| `test_value_encoding_verification` | ...oding/src/encodings/physical/value.rs | medium | unit |
| `test_will_bitpack_allowed_types_when_possible` | ...revious/encodings/physical/bitpack.rs | medium | unit |
| `test_basic_rle_encoding` | ...ncoding/src/encodings/physical/rle.rs | simple | unit |
| `test_blob_encoding_simple` | ...ncoding/src/encodings/logical/blob.rs | simple | unit |
| `test_compressed_mini_block_empty_data` | ...ing/src/encodings/physical/general.rs | simple | unit |
| `test_dict_encoding_should_not_be_applied_for_em...` | lance-encoding/src/previous/encoder.rs | simple | unit |
| `test_list_dict_empty_batch` | ...-encoding/src/encodings/fuzz_tests.rs | simple | unit |
| `test_simple_already_dictionary` | ...ious/encodings/physical/dictionary.rs | simple | unit |
| `test_simple_list_dict` | ...ncoding/src/encodings/logical/list.rs | simple | unit |

#### schema (9)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bss_field_metadata` | lance-encoding/src/compression.rs | medium | unit |
| `test_choose_encoder_for_zstd_compressed_string_...` | lance-encoding/src/previous/encoder.rs | medium | unit |
| `test_field_metadata_compression` | lance-encoding/src/compression.rs | medium | unit |
| `test_field_metadata_mixed_configuration` | lance-encoding/src/compression.rs | medium | unit |
| `test_field_metadata_none_compression` | lance-encoding/src/compression.rs | medium | unit |
| `test_field_metadata_override_params` | lance-encoding/src/compression.rs | medium | unit |
| `test_field_metadata_rle_threshold` | lance-encoding/src/compression.rs | medium | unit |
| `test_field_params_merge` | lance-encoding/src/compression_config.rs | medium | unit |
| `test_get_field_params` | lance-encoding/src/compression_config.rs | medium | unit |

#### index (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_fullzip_repetition_index_caching` | ...ng/src/encodings/logical/primitive.rs | complex | unit |
| `test_control_words_rep_index` | lance-encoding/src/repdef.rs | medium | unit |
| `test_coalesce_indices_to_ranges_with_single_index` | lance-encoding/src/decoder.rs | simple | unit |

#### scan (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_filter_nested_fsl_garbage` | .../encodings/logical/fixed_size_list.rs | medium | unit |
| `test_filter_nested_fsl_no_list_child` | .../encodings/logical/fixed_size_list.rs | medium | unit |

#### update (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_parameter_merge_priority` | lance-encoding/src/compression.rs | medium | unit |

#### io (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_map_not_supported_write_in_v2_1` | ...encoding/src/encodings/logical/map.rs | medium | unit |

#### arrow (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_is_narrow` | ...ng/src/encodings/logical/primitive.rs | medium | unit |

### lance-io (97 tests)

#### other (56)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `stress_backpressure` | lance-io/src/scheduler.rs | complex | unit |
| `test_concurrent_access` | ...o/src/object_store/storage_options.rs | complex | unit |
| `test_dynamic_credential_provider_concurrent_access` | ...-io/src/object_store/providers/aws.rs | complex | unit |
| `test_dynamic_credential_provider_concurrent_ref...` | ...-io/src/object_store/providers/aws.rs | complex | unit |
| `test_large_upload` | lance-io/tests/gcs_integration.rs | complex | integration |
| `calculate_prefix_uses_repo_id` | ...object_store/providers/huggingface.rs | medium | unit |
| `extract_path_returns_relative` | ...object_store/providers/huggingface.rs | medium | unit |
| `parse_hf_repo_id_legacy_without_type` | ...object_store/providers/huggingface.rs | medium | unit |
| `parse_hf_repo_id_missing_segments_errors` | ...object_store/providers/huggingface.rs | medium | unit |
| `parse_hf_repo_id_strips_revision` | ...object_store/providers/huggingface.rs | medium | unit |
| `parse_hf_repo_id_with_type_and_owner_repo` | ...object_store/providers/huggingface.rs | medium | unit |
| `parse_invalid_url_errors` | ...object_store/providers/huggingface.rs | medium | unit |
| `storage_option_revision_takes_precedence` | ...object_store/providers/huggingface.rs | medium | unit |
| `test_absolute_paths` | lance-io/src/object_store.rs | medium | unit |
| `test_accessor_id_static` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_accessor_id_with_provider` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_accessor_used_when_no_explicit_aws_credent...` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_backpressure` | lance-io/src/scheduler.rs | medium | unit |
| `test_block_size_used_cloud` | lance-io/src/object_store.rs | medium | unit |
| `test_caching_and_refresh` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_cloud_paths` | lance-io/src/object_store.rs | medium | unit |
| `test_copy_creates_parent_directories` | lance-io/src/object_store.rs | medium | unit |
| `test_dynamic_credential_provider_no_initial_cache` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_dynamic_credential_provider_refresh_lead_time` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_dynamic_credential_provider_with_expired_c...` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_dynamic_credential_provider_with_initial_c...` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_dynamic_credential_provider_with_initial_o...` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_expired_initial_triggers_refresh` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_explicit_aws_credentials_takes_precedence_...` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_find_configured_storage_account` | ...o/src/object_store/providers/azure.rs | medium | unit |
| `test_initial_and_provider_uses_initial_first` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_injected_aws_creds_option_is_used` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_is_s3_express` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_local_paths` | lance-io/src/object_store.rs | medium | unit |
| `test_make_chunked_request` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_no_expiration_never_refreshes` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_params_slice` | lance-io/src/lib.rs | medium | unit |
| `test_params_to_offsets` | lance-io/src/lib.rs | medium | unit |
| `test_priority` | lance-io/src/scheduler.rs | medium | unit |
| `test_provider_only` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_range_query` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_relative_paths` | lance-io/src/object_store.rs | medium | unit |
| `test_s3_path_parsing` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_small_upload` | lance-io/tests/gcs_integration.rs | medium | integration |
| `test_split_coalesce` | lance-io/src/scheduler.rs | medium | unit |
| `test_static_options_only` | ...o/src/object_store/storage_options.rs | medium | unit |
| `test_take` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_take` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_take_dense_indices` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_take_sparse_indices` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_tilde_expansion` | lance-io/src/object_store.rs | medium | unit |
| `test_use_opendal_flag` | ...-io/src/object_store/providers/aws.rs | medium | unit |
| `test_use_opendal_flag` | ...o/src/object_store/providers/azure.rs | medium | unit |
| `test_use_opendal_flag` | ...-io/src/object_store/providers/gcp.rs | medium | unit |
| `test_windows_paths` | lance-io/src/object_store.rs | medium | unit |
| `parse_basic_url` | ...object_store/providers/huggingface.rs | simple | unit |

#### io (31)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_full_seq_read` | lance-io/src/scheduler.rs | complex | unit |
| `test_abort_write` | lance-io/src/object_writer.rs | medium | unit |
| `test_azure_store_path` | ...o/src/object_store/providers/azure.rs | medium | unit |
| `test_block_size_used_file` | lance-io/src/object_store.rs | medium | unit |
| `test_calculate_object_store_prefix` | lance-io/src/object_store/providers.rs | medium | unit |
| `test_calculate_object_store_prefix` | ...o/src/object_store/providers/local.rs | medium | unit |
| `test_calculate_object_store_prefix` | .../src/object_store/providers/memory.rs | medium | unit |
| `test_calculate_object_store_prefix_for_dummy_path` | lance-io/src/object_store/providers.rs | medium | unit |
| `test_calculate_object_store_prefix_for_file_obj...` | ...o/src/object_store/providers/local.rs | medium | unit |
| `test_calculate_object_store_prefix_for_local` | lance-io/src/object_store/providers.rs | medium | unit |
| `test_calculate_object_store_prefix_for_local_wi...` | lance-io/src/object_store/providers.rs | medium | unit |
| `test_calculate_object_store_prefix_from_url_and...` | ...o/src/object_store/providers/azure.rs | medium | unit |
| `test_calculate_object_store_prefix_from_url_and...` | ...o/src/object_store/providers/azure.rs | medium | unit |
| `test_calculate_object_store_scheme_not_found` | lance-io/src/object_store/providers.rs | medium | unit |
| `test_cross_filesystem_copy` | lance-io/src/object_store.rs | medium | unit |
| `test_fail_to_calculate_object_store_prefix_from...` | ...o/src/object_store/providers/azure.rs | medium | unit |
| `test_file_store_path` | ...o/src/object_store/providers/local.rs | medium | unit |
| `test_file_store_path_windows` | ...o/src/object_store/providers/local.rs | medium | unit |
| `test_gcs_store_path` | ...-io/src/object_store/providers/gcp.rs | medium | unit |
| `test_list_retry_stream_send` | lance-io/src/object_store/list_retry.rs | medium | unit |
| `test_memory_store_path` | .../src/object_store/providers/memory.rs | medium | unit |
| `test_oss_store_path` | ...-io/src/object_store/providers/oss.rs | medium | unit |
| `test_read_directory` | lance-io/src/object_store.rs | medium | unit |
| `test_read_one` | lance-io/src/object_store.rs | medium | unit |
| `test_wrapping_object_store_option_is_used` | lance-io/src/object_store.rs | medium | unit |
| `test_write` | lance-io/src/object_writer.rs | medium | unit |
| `test_write_binary_data` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_write_binary_data_with_offset` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_write_binary_with_nulls` | lance-io/src/encodings/binary.rs | medium | unit |
| `test_write_proto_structs` | lance-io/src/utils.rs | medium | unit |
| `test_write_slice` | lance-io/src/encodings/binary.rs | medium | unit |

#### encoding (7)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_decode_by_range` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_dict_decoder` | lance-io/src/encodings/dictionary.rs | medium | unit |
| `test_encode_decode_bool_array` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_encode_decode_fixed_size_binary_array` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_encode_decode_fixed_size_list_array` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_encode_decode_nested_fixed_size_list` | lance-io/src/encodings/plain.rs | medium | unit |
| `test_encode_decode_primitive_array` | lance-io/src/encodings/plain.rs | medium | unit |

#### delete (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_delete_directory` | lance-io/src/object_store.rs | medium | unit |

#### append (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_bytes_to_array_padding` | lance-io/src/encodings/plain.rs | medium | unit |

#### statistics (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_stats_hit_miss_tracking` | lance-io/src/object_store/providers.rs | medium | unit |

### lance-file (56 tests)

#### other (16)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `drop_while_scheduling` | lance-file/src/reader.rs | medium | unit |
| `test_blocking_take` | lance-file/src/reader.rs | medium | unit |
| `test_drop_in_progress` | lance-file/src/reader.rs | medium | unit |
| `test_edge_cases` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_error_handling` | lance-file/src/previous/page_table.rs | medium | unit |
| `test_global_buffers` | lance-file/src/reader.rs | medium | unit |
| `test_list_array_with_offsets` | lance-file/src/previous/reader.rs | medium | unit |
| `test_max_page_bytes_enforced` | lance-file/src/writer.rs | medium | unit |
| `test_max_page_bytes_env_var` | lance-file/src/writer.rs | medium | unit |
| `test_no_arrays` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_round_trip` | lance-file/src/reader.rs | medium | unit |
| `test_roundtrip_page_info` | lance-file/src/previous/page_table.rs | medium | integration |
| `test_set_page_info` | lance-file/src/previous/page_table.rs | medium | unit |
| `test_take` | lance-file/src/previous/reader.rs | medium | unit |
| `test_take_boolean_beyond_chunk` | lance-file/src/previous/reader.rs | medium | unit |
| `test_take_lists` | lance-file/src/previous/reader.rs | medium | unit |

#### io (13)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_large_page_split_on_read` | lance-file/src/writer.rs | complex | unit |
| `read_nullable_string_in_struct` | lance-file/src/previous/reader.rs | medium | unit |
| `test_batches_stream` | lance-file/src/previous/reader.rs | medium | unit |
| `test_read_all` | lance-file/src/reader.rs | medium | unit |
| `test_read_nullable_arrays` | lance-file/src/previous/reader.rs | medium | unit |
| `test_read_projection` | lance-file/src/previous/reader.rs | medium | unit |
| `test_read_ranges` | lance-file/src/previous/reader.rs | medium | unit |
| `test_read_struct_of_list_arrays` | lance-file/src/previous/reader.rs | medium | unit |
| `test_write_file` | lance-file/src/previous/writer/mod.rs | medium | unit |
| `test_write_temporal_types` | lance-file/src/previous/writer/mod.rs | medium | unit |
| `test_basic_write` | lance-file/src/writer.rs | simple | unit |
| `test_read_empty_range` | lance-file/src/reader.rs | simple | unit |
| `test_write_empty` | lance-file/src/writer.rs | simple | unit |

#### statistics (12)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_boolean_statistics_multi_array` | ...ile/src/previous/writer/statistics.rs | complex | unit |
| `test_collect_binary_stats` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_collect_float_stats` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_collect_primitive_stats` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_collect_stats` | lance-file/src/previous/writer/mod.rs | medium | unit |
| `test_min_max_constant_property` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_min_max_constant_property_timestamp` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_min_max_ordering_binary` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_min_max_ordering_float` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_min_max_ordering_fsb` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_min_max_ordering_string` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_stats_collector` | ...ile/src/previous/writer/statistics.rs | medium | unit |

#### encoding (6)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_compression_overrides_end_to_end` | lance-file/src/writer.rs | complex | integration |
| `test_collect_dictionary_stats` | ...ile/src/previous/writer/statistics.rs | medium | unit |
| `test_compressing_buffer` | lance-file/src/reader.rs | medium | unit |
| `test_dictionary_first_element_file` | lance-file/src/previous/writer/mod.rs | medium | unit |
| `test_encode_slice` | lance-file/src/previous/writer/mod.rs | medium | unit |
| `test_encoded_batch_round_trip` | lance-file/src/reader.rs | medium | unit |

#### schema (5)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_field_metadata_compression` | lance-file/src/writer.rs | medium | unit |
| `test_field_metadata_rle_threshold` | lance-file/src/writer.rs | medium | unit |
| `test_schema_metadata` | lance-file/src/datatypes.rs | medium | unit |
| `test_schema_set_ids` | lance-file/src/datatypes.rs | medium | unit |
| `test_write_schema_with_holes` | lance-file/src/previous/writer/mod.rs | medium | unit |

#### scan (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_projection` | lance-file/src/reader.rs | medium | unit |
| `test_scan_struct_of_list_arrays` | lance-file/src/previous/reader.rs | medium | unit |

#### arrow (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_group_indices_to_batch` | ...-file/src/previous/format/metadata.rs | medium | unit |
| `test_range_to_batches` | ...-file/src/previous/format/metadata.rs | medium | unit |

### lance-arrow (64 tests)

#### other (28)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_try_from_large_string_array` | lance-arrow/src/json.rs | complex | unit |
| `test_try_from_large_string_array_invalid_json` | lance-arrow/src/json.rs | complex | unit |
| `test_byte_width_opt` | lance-arrow/src/lib.rs | medium | unit |
| `test_coerce_float_vector_bfloat16` | lance-arrow/src/floats.rs | medium | unit |
| `test_deep_copy_array_data_sliced` | lance-arrow/src/deepcopy.rs | medium | unit |
| `test_deep_copy_array_sliced` | lance-arrow/src/deepcopy.rs | medium | unit |
| `test_deep_copy_array_sliced_with_nulls` | lance-arrow/src/deepcopy.rs | medium | unit |
| `test_deep_copy_sliced_array_with_nulls` | lance-arrow/src/deepcopy.rs | medium | unit |
| `test_json_array_from_invalid_json` | lance-arrow/src/json.rs | medium | unit |
| `test_json_array_from_string_array` | lance-arrow/src/json.rs | medium | unit |
| `test_json_array_from_strings` | lance-arrow/src/json.rs | medium | unit |
| `test_json_array_inner` | lance-arrow/src/json.rs | medium | unit |
| `test_json_array_trait_methods` | lance-arrow/src/json.rs | medium | unit |
| `test_json_array_value_bytes` | lance-arrow/src/json.rs | medium | unit |
| `test_json_array_value_null_error` | lance-arrow/src/json.rs | medium | unit |
| `test_json_path_extraction` | lance-arrow/src/json.rs | medium | unit |
| `test_json_path_invalid_path` | lance-arrow/src/json.rs | medium | unit |
| `test_json_path_on_corrupted_jsonb` | lance-arrow/src/json.rs | medium | unit |
| `test_json_path_with_null` | lance-arrow/src/json.rs | medium | unit |
| `test_memory_accumulator` | lance-arrow/src/memory.rs | medium | unit |
| `test_normalize_slicing_no_offset` | lance-arrow/src/struct.rs | medium | unit |
| `test_nulls` | lance-arrow/src/bfloat16.rs | medium | unit |
| `test_project_preserves_struct_validity` | lance-arrow/src/lib.rs | medium | unit |
| `test_trim_values` | lance-arrow/src/list.rs | medium | unit |
| `test_try_from_array_ref` | lance-arrow/src/json.rs | medium | unit |
| `test_try_from_string_array_invalid_json` | lance-arrow/src/json.rs | medium | unit |
| `test_basics` | lance-arrow/src/bfloat16.rs | simple | unit |
| `test_json_array_empty` | lance-arrow/src/json.rs | simple | unit |

#### schema (21)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_convert_json_columns_invalid_json_large_utf8` | lance-arrow/src/json.rs | complex | unit |
| `test_convert_json_columns_large_string` | lance-arrow/src/json.rs | complex | unit |
| `test_is_arrow_json_field_large_utf8` | lance-arrow/src/json.rs | complex | unit |
| `test_arrow_json_to_lance_json_non_json_field` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_json_columns` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_json_columns_invalid_json_utf8` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_json_columns_invalid_storage_type` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_json_columns_mixed_columns` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_json_columns_no_json_columns` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_lance_json_to_arrow_mixed_columns` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_lance_json_to_arrow_no_json_columns` | lance-arrow/src/json.rs | medium | unit |
| `test_has_json_fields` | lance-arrow/src/json.rs | medium | unit |
| `test_is_arrow_json_field_wrong_extension` | lance-arrow/src/json.rs | medium | unit |
| `test_is_json_field_wrong_extension` | lance-arrow/src/json.rs | medium | unit |
| `test_json_field_creation` | lance-arrow/src/json.rs | medium | unit |
| `test_merge_with_schema` | lance-arrow/src/lib.rs | medium | unit |
| `test_merge_with_schema_with_nullable_struct_lis...` | lance-arrow/src/lib.rs | medium | unit |
| `test_project_by_schema_list_struct_reorder` | lance-arrow/src/lib.rs | medium | unit |
| `test_project_by_schema_nested_list_struct` | lance-arrow/src/lib.rs | medium | unit |
| `test_schema_project_by_schema` | lance-arrow/src/lib.rs | medium | unit |
| `test_convert_json_columns_empty_batch` | lance-arrow/src/json.rs | simple | unit |

#### arrow (7)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_arrow_cpp_slicing` | lance-arrow/src/struct.rs | medium | unit |
| `test_arrow_rs_slicing` | lance-arrow/src/struct.rs | medium | unit |
| `test_convert_lance_json_to_arrow` | lance-arrow/src/json.rs | medium | unit |
| `test_deep_copy_batch_sliced` | lance-arrow/src/deepcopy.rs | medium | unit |
| `test_take_record_batch` | lance-arrow/src/lib.rs | medium | unit |
| `test_to_arrow_json` | lance-arrow/src/json.rs | medium | unit |
| `test_convert_lance_json_to_arrow_empty_batch` | lance-arrow/src/json.rs | simple | unit |

#### update (5)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_merge_struct_large_lists` | lance-arrow/src/lib.rs | complex | unit |
| `test_merge_list_struct` | lance-arrow/src/lib.rs | medium | unit |
| `test_merge_recursive` | lance-arrow/src/lib.rs | medium | unit |
| `test_merge_struct_lists` | lance-arrow/src/lib.rs | medium | unit |
| `test_merge_struct_with_different_validity` | lance-arrow/src/lib.rs | medium | unit |

#### encoding (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_decode_json_on_various_inputs` | lance-arrow/src/json.rs | medium | unit |
| `test_encode_json_invalid` | lance-arrow/src/json.rs | medium | unit |

#### scan (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_filter_garbage_nulls` | lance-arrow/src/list.rs | medium | unit |

### lance-namespace (18 tests)

#### other (15)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_convert_to_lance_error` | lance-namespace/src/error.rs | medium | unit |
| `test_error_code_roundtrip` | lance-namespace/src/error.rs | medium | integration |
| `test_error_display` | lance-namespace/src/error.rs | medium | unit |
| `test_extension_metadata_preserved_in_json_round...` | lance-namespace/src/schema.rs | medium | integration |
| `test_fixed_size_list_type` | lance-namespace/src/schema.rs | medium | unit |
| `test_from_code` | lance-namespace/src/error.rs | medium | unit |
| `test_from_unknown_code` | lance-namespace/src/error.rs | medium | unit |
| `test_list_type` | lance-namespace/src/schema.rs | medium | unit |
| `test_map_type_supported` | lance-namespace/src/schema.rs | medium | unit |
| `test_namespace_error_code` | lance-namespace/src/error.rs | medium | unit |
| `test_nested_struct_with_list` | lance-namespace/src/schema.rs | medium | unit |
| `test_struct_type` | lance-namespace/src/schema.rs | medium | unit |
| `test_unknown_error_code` | lance-namespace/src/error.rs | medium | unit |
| `test_unsupported_type` | lance-namespace/src/schema.rs | medium | unit |
| `test_convert_basic_types` | lance-namespace/src/schema.rs | simple | unit |

#### schema (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_convert_field` | lance-namespace/src/schema.rs | medium | unit |
| `test_convert_schema` | lance-namespace/src/schema.rs | medium | unit |

#### append (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_additional_types` | lance-namespace/src/schema.rs | medium | unit |

### lance-namespace-impls (164 tests)

#### other (140)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_construct_full_uri_with_cloud_urls` | ...e-namespace-impls/src/dir/manifest.rs | complex | unit |
| `test_multiple_tables_in_child_namespace` | lance-namespace-impls/src/dir.rs | complex | unit |
| `test_atomic_table_status_check` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_build_access_boundary_admin` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_build_access_boundary_no_prefix` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_build_cache_key_no_identity` | ...espace-impls/src/credentials/cache.rs | medium | unit |
| `test_build_cache_key_with_identity` | ...espace-impls/src/credentials/cache.rs | medium | unit |
| `test_build_policy_admin` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_build_sas_permissions_admin` | ...espace-impls/src/credentials/azure.rs | medium | unit |
| `test_caching_reduces_calls` | ...espace-impls/src/credentials/cache.rs | medium | unit |
| `test_calculate_cache_ttl` | ...espace-impls/src/credentials/cache.rs | medium | unit |
| `test_cannot_drop_namespace_with_tables` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_child_namespace_create_and_list` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_clear_cache` | ...espace-impls/src/credentials/cache.rs | medium | unit |
| `test_config_builder` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_config_builder` | ...espace-impls/src/credentials/azure.rs | medium | unit |
| `test_config_builder` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_config_custom_root` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_config_storage_options` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_connect_builder_invalid_impl` | lance-namespace-impls/src/connect.rs | medium | unit |
| `test_connect_builder_with_properties` | lance-namespace-impls/src/connect.rs | medium | unit |
| `test_connect_builder_with_session` | lance-namespace-impls/src/connect.rs | medium | unit |
| `test_connect_dir` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_context_headers_override_base_headers` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_context_provider_headers_sent` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_create_and_list_child_namespaces` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_create_child_namespace` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_create_duplicate_table_fails` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_create_namespace_success` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_create_namespace_without_parent_fails` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_create_nested_namespace` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_create_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_create_table_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_create_table_in_child_namespace` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_create_table_success` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_create_table_with_invalid_id` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_create_table_with_ipc_data` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_create_table_without_data` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_credential_refresh_on_expiration` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_custom_headers_are_sent` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_declare_table_v1_mode` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_declare_table_when_table_exists` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_deeply_nested_namespace` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_deeply_nested_namespace_with_table` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_default_configuration` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_deregister_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_deregister_table` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_deregister_table_in_child_namespace` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_deregister_table_v1_mode` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_describe_child_namespace` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_describe_nonexistent_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_describe_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_describe_table_fails_for_deregistered_v1` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_describe_table_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_detect_provider_from_uri` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_different_identities_cached_separately` | ...espace-impls/src/credentials/cache.rs | medium | unit |
| `test_directory_namespace_with_aws_credential_ve...` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_directory_only_mode` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_drop_child_namespace` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_drop_namespace_with_children_fails` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_drop_namespace_with_tables_fails` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_drop_nonexistent_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_drop_nonexistent_table` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_drop_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_drop_table_in_child_namespace` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_drop_table_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_from_properties_builder` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_from_properties_defaults` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_from_properties_dir_listing_enabled` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_from_properties_with_storage_options` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_has_credential_vendor_config` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_isolation_between_namespaces` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_list_namespaces_error` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_list_namespaces_success` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_list_tables` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_list_tables_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_list_tables_skips_deregistered_v1` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_list_tables_with_namespace_id` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_migrate_directory_tables` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_namespace_creation_with_tls_config` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_namespace_isolation` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_namespace_with_properties` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_namespace_with_properties` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_nested_namespace_hierarchy` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_nested_namespace_hierarchy` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_no_context_provider_uses_base_headers_only` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_non_root_namespace_operations` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_normalize_workload_identity_audience` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_operation_info_creation` | lance-namespace-impls/src/context.rs | medium | unit |
| `test_parse_duration_millis_with_invalid_values` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_parse_gcs_uri` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_parse_gcs_uri_invalid` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_parse_id_custom_delimiter` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_parse_id_default_delimiter` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_parse_id_root_namespace` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_parse_permission_with_invalid_values` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_parse_s3_uri` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_redact_credential` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_register_deregister_round_trip` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_register_table` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_register_table` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_register_table_duplicate_fails` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_register_table_rejects_absolute_path` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_register_table_rejects_absolute_uri` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_register_table_rejects_absolute_uri` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_register_table_rejects_path_traversal` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_register_table_rejects_path_traversal` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_rest_namespace_creation` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_rest_namespace_with_context_provider` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_root_namespace_operations` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_root_namespace_operations` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_scoped_policy_permissions` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_table_exists` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_table_exists_fails_for_deregistered_v1` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_table_exists_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_table_in_child_namespace` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_tls_config_default_assert_hostname` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_tls_config_disable_hostname_verification` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_tls_config_parsing` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_trailing_slash_handling` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |
| `test_vended_credentials_debug_redacts_secrets` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_vended_credentials_is_expired` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_vended_permission_display` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_vended_permission_from_str` | lance-namespace-impls/src/credentials.rs | medium | unit |
| `test_with_custom_uri` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_aws_credential_vending_basic` | ...amespace-impls/src/credentials/aws.rs | simple | unit |
| `test_connect_builder_basic` | lance-namespace-impls/src/connect.rs | simple | unit |
| `test_context_provider_basic` | lance-namespace-impls/src/context.rs | simple | unit |
| `test_create_empty_table` | lance-namespace-impls/src/dir.rs | simple | unit |
| `test_create_empty_table_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |
| `test_create_empty_table_then_drop` | lance-namespace-impls/src/dir.rs | simple | unit |
| `test_create_empty_table_with_wrong_location` | lance-namespace-impls/src/dir.rs | simple | unit |
| `test_deeply_nested_namespace_with_empty_table` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |
| `test_describe_empty_table_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |
| `test_drop_empty_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |
| `test_drop_empty_table_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |
| `test_empty_context` | lance-namespace-impls/src/context.rs | simple | unit |
| `test_empty_table_exists_in_child_namespace` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |
| `test_empty_table_in_child_namespace` | lance-namespace-impls/src/dir.rs | simple | unit |
| `test_parse_id_single_part` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |

#### manifest (10)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_manifest_namespace_multiple_tables` | ...e-namespace-impls/src/dir/manifest.rs | complex | unit |
| `test_declare_table_with_manifest` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_from_properties_manifest_enabled` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_manifest_namespace_describe_table` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_manifest_namespace_drop_table` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_manifest_namespace_table_exists` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_manifest_only_mode` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |
| `test_migrate_without_manifest` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_register_without_manifest_fails` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_manifest_namespace_basic_create_and_list` | ...e-namespace-impls/src/dir/manifest.rs | simple | unit |

#### io (9)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_build_access_boundary_read` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_build_access_boundary_write` | ...amespace-impls/src/credentials/gcp.rs | medium | unit |
| `test_build_policy_read` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_build_policy_write` | ...amespace-impls/src/credentials/aws.rs | medium | unit |
| `test_build_sas_permissions_read` | ...espace-impls/src/credentials/azure.rs | medium | unit |
| `test_build_sas_permissions_write` | ...espace-impls/src/credentials/azure.rs | medium | unit |
| `test_deregister_table_v1_already_deregistered` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_namespace_write` | lance-namespace-impls/src/dir.rs | medium | unit |
| `test_namespace_write` | ...e-namespace-impls/src/rest_adapter.rs | medium | unit |

#### update (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_base_headers_merged_with_context_headers` | lance-namespace-impls/src/rest.rs | medium | unit |
| `test_dual_mode_merge` | ...e-namespace-impls/src/dir/manifest.rs | medium | unit |

#### append (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_insert_into_table_success` | lance-namespace-impls/src/rest.rs | medium | unit |

#### arrow (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_various_arrow_types` | lance-namespace-impls/src/dir.rs | medium | unit |

#### scan (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_parse_id_filters_empty` | ...e-namespace-impls/src/rest_adapter.rs | simple | unit |

### lance-datafusion (53 tests)

#### other (36)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_resolve_large_utf8` | lance-datafusion/src/logical_expr.rs | complex | unit |
| `test_chunkers` | lance-datafusion/src/chunker.rs | medium | unit |
| `test_contains_tokens` | lance-datafusion/src/udf.rs | medium | unit |
| `test_double_equal` | lance-datafusion/src/sql.rs | medium | unit |
| `test_expr_substrait_roundtrip` | lance-datafusion/src/substrait.rs | medium | integration |
| `test_json_array_access` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_array_contains_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_array_length_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_exists_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_extract_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_get_bool_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_get_int_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_json_get_string_udf` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_jsonb_type_enum` | lance-datafusion/src/udf/json.rs | medium | unit |
| `test_lance_context_provider_expr_planners` | lance-datafusion/src/planner.rs | medium | unit |
| `test_like` | lance-datafusion/src/sql.rs | medium | unit |
| `test_negative_array_expressions` | lance-datafusion/src/planner.rs | medium | unit |
| `test_negative_expressions` | lance-datafusion/src/planner.rs | medium | unit |
| `test_nested_col_refs` | lance-datafusion/src/planner.rs | medium | unit |
| `test_nested_list_refs` | lance-datafusion/src/planner.rs | medium | unit |
| `test_not_like` | lance-datafusion/src/planner.rs | medium | unit |
| `test_parse_binary_expr` | lance-datafusion/src/planner.rs | medium | unit |
| `test_quoted_ident` | lance-datafusion/src/sql.rs | medium | unit |
| `test_regexp_match_infer_error_without_boolean_c...` | lance-datafusion/src/planner.rs | medium | unit |
| `test_resolve_binary_expr_on_right` | lance-datafusion/src/logical_expr.rs | medium | unit |
| `test_resolve_in_expr` | lance-datafusion/src/logical_expr.rs | medium | unit |
| `test_session_context_cache` | lance-datafusion/src/exec.rs | medium | unit |
| `test_session_context_cache_lru_eviction` | lance-datafusion/src/exec.rs | medium | unit |
| `test_spill` | lance-datafusion/src/spill.rs | medium | unit |
| `test_spill_buffered` | lance-datafusion/src/spill.rs | medium | unit |
| `test_spill_buffered_transition` | lance-datafusion/src/spill.rs | medium | unit |
| `test_spill_error` | lance-datafusion/src/spill.rs | medium | unit |
| `test_substrait_roundtrip_with_list_of_struct` | lance-datafusion/src/substrait.rs | medium | integration |
| `test_substrait_roundtrip_with_list_struct_struct` | lance-datafusion/src/substrait.rs | medium | integration |
| `test_temporal_coerce` | lance-datafusion/src/expr.rs | medium | unit |
| `test_regexp_match_and_non_empty_captions` | lance-datafusion/src/planner.rs | simple | unit |

#### index (9)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_sql_array_literals` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_between` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_cast` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_comparison` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_invert` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_is_in` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_is_null` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_like` | lance-datafusion/src/planner.rs | medium | unit |
| `test_sql_literals` | lance-datafusion/src/planner.rs | medium | unit |

#### schema (4)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_substrait_roundtrip_with_many_nested_columns` | lance-datafusion/src/substrait.rs | complex | integration |
| `test_columns_in_expr` | lance-datafusion/src/planner.rs | medium | unit |
| `test_output_schema_preserves_json_extension_met...` | lance-datafusion/src/projection.rs | medium | unit |
| `test_resolve_column_type` | lance-datafusion/src/logical_expr.rs | medium | unit |

#### io (2)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_group_by_stream` | lance-datafusion/src/dataframe.rs | medium | unit |
| `test_strict_batch_size_stream` | lance-datafusion/src/chunker.rs | medium | unit |

#### manifest (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_substrait_conversion` | lance-datafusion/src/substrait.rs | medium | unit |

#### scan (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_parse_filter_simple` | lance-datafusion/src/planner.rs | simple | unit |

### lance-linalg (44 tests)

#### other (43)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_cosine_large` | lance-linalg/src/distance/cosine.rs | complex | unit |
| `test_argmax` | lance-linalg/src/kernels.rs | medium | unit |
| `test_argmin` | lance-linalg/src/kernels.rs | medium | unit |
| `test_cosine` | lance-linalg/src/distance/cosine.rs | medium | unit |
| `test_cosine_bf16` | lance-linalg/src/distance/cosine.rs | medium | unit |
| `test_cosine_f16` | lance-linalg/src/distance/cosine.rs | medium | unit |
| `test_cosine_f32` | lance-linalg/src/distance/cosine.rs | medium | unit |
| `test_cosine_f64` | lance-linalg/src/distance/cosine.rs | medium | unit |
| `test_cosine_not_aligned` | lance-linalg/src/distance/cosine.rs | medium | unit |
| `test_dot` | lance-linalg/src/distance/dot.rs | medium | unit |
| `test_dot_bf16` | lance-linalg/src/distance/dot.rs | medium | unit |
| `test_dot_f16` | lance-linalg/src/distance/dot.rs | medium | unit |
| `test_dot_f32` | lance-linalg/src/distance/dot.rs | medium | unit |
| `test_dot_f64` | lance-linalg/src/distance/dot.rs | medium | unit |
| `test_euclidean_distance` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_f32x16_cmp_ops` | lance-linalg/src/simd/f32.rs | medium | unit |
| `test_f32x8_cmp_ops` | lance-linalg/src/simd/f32.rs | medium | unit |
| `test_f32x8_gather` | lance-linalg/src/simd/f32.rs | medium | unit |
| `test_hamming` | lance-linalg/src/distance/hamming.rs | medium | unit |
| `test_hash_unsupported_type` | lance-linalg/src/kernels.rs | medium | unit |
| `test_l2_distance_bf16` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_l2_distance_cases` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_l2_distance_f16` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_l2_distance_f16_max` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_l2_distance_f32` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_l2_distance_f64` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_l2_norm_bf16` | lance-linalg/src/distance/norm_l2.rs | medium | unit |
| `test_l2_norm_f16` | lance-linalg/src/distance/norm_l2.rs | medium | unit |
| `test_l2_norm_f32` | lance-linalg/src/distance/norm_l2.rs | medium | unit |
| `test_l2_norm_f64` | lance-linalg/src/distance/norm_l2.rs | medium | unit |
| `test_normalize_fsl_edge_cases` | lance-linalg/src/kernels.rs | medium | unit |
| `test_normalize_fsl_with_nulls` | lance-linalg/src/kernels.rs | medium | unit |
| `test_normalize_vector` | lance-linalg/src/kernels.rs | medium | unit |
| `test_not_aligned` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_numeric_hashes` | lance-linalg/src/kernels.rs | medium | unit |
| `test_odd_length_vector` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_string_hashes` | lance-linalg/src/kernels.rs | medium | unit |
| `test_uint8_l2_edge_cases` | lance-linalg/src/distance/l2.rs | medium | unit |
| `test_basic_f32x16_ops` | lance-linalg/src/simd/f32.rs | simple | unit |
| `test_basic_ops` | lance-linalg/src/simd/f32.rs | simple | unit |
| `test_basic_u8x16_ops` | lance-linalg/src/simd/u8.rs | simple | unit |
| `test_multivec_distance_empty_row_is_nan` | lance-linalg/src/distance.rs | simple | unit |
| `test_sum_4bit_dist_table_basic` | lance-linalg/src/simd/dist_table.rs | simple | unit |

#### append (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_saturating_add` | lance-linalg/src/simd/u8.rs | medium | unit |

### lance-datagen (11 tests)

#### other (10)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_cycle` | lance-datagen/src/generator.rs | medium | unit |
| `test_fill` | lance-datagen/src/generator.rs | medium | unit |
| `test_jitter_centroids` | lance-datagen/src/generator.rs | medium | unit |
| `test_nulls` | lance-datagen/src/generator.rs | medium | unit |
| `test_rng` | lance-datagen/src/generator.rs | medium | unit |
| `test_rng_distribution` | lance-datagen/src/generator.rs | medium | unit |
| `test_rng_list` | lance-datagen/src/generator.rs | medium | unit |
| `test_step` | lance-datagen/src/generator.rs | medium | unit |
| `test_unit_circle` | lance-datagen/src/generator.rs | medium | unit |
| `test_utf8_prefix_plus_counter` | lance-datagen/src/generator.rs | medium | unit |

#### schema (1)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_rand_schema` | lance-datagen/src/generator.rs | medium | unit |

### lance-tools (3 tests)

#### other (3)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_error_lance_result_to_error_std_result` | lance-tools/src/main.rs | medium | unit |
| `test_ok_lance_result_to_ok_std_result` | lance-tools/src/main.rs | medium | unit |
| `test_path_to_parent` | lance-tools/src/util.rs | medium | unit |

### compression (5 tests)

#### other (5)

| 测试名称 | 文件 | 复杂度 | 范围 |
|----------|------|--------|------|
| `test_fsst` | compression/fsst/src/fsst.rs | medium | unit |
| `test_fsst_64_bit_offsets` | compression/fsst/src/fsst.rs | medium | unit |
| `test_pack` | compression/bitpacking/src/lib.rs | medium | unit |
| `test_symbol_new` | compression/fsst/src/fsst.rs | medium | unit |
| `test_unchecked_pack` | compression/bitpacking/src/lib.rs | medium | unit |
