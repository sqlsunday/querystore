WITH waits AS (
    SELECT plan_id, runtime_stats_interval_id,
           SUM((CASE WHEN wait_category=1  THEN total_query_wait_time_ms ELSE 0 END)) AS sos_scheduler_yield,   -- CPU
           SUM((CASE WHEN wait_category=2  THEN total_query_wait_time_ms ELSE 0 END)) AS threadpool,            -- Thread starvation
           SUM((CASE WHEN wait_category=3  THEN total_query_wait_time_ms ELSE 0 END)) AS lock,                  -- Locks
           SUM((CASE WHEN wait_category=4  THEN total_query_wait_time_ms ELSE 0 END)) AS latch,                 -- Latches
           SUM((CASE WHEN wait_category=5  THEN total_query_wait_time_ms ELSE 0 END)) AS pagelatch,             -- Buffer latch
           SUM((CASE WHEN wait_category=6  THEN total_query_wait_time_ms ELSE 0 END)) AS pageiolatch,           -- Buffer IO
           SUM((CASE WHEN wait_category=7  THEN total_query_wait_time_ms ELSE 0 END)) AS compile,               -- Compile
           SUM((CASE WHEN wait_category=14 THEN total_query_wait_time_ms ELSE 0 END)) AS transaction_log,       -- Logwrite, checkpoint, etc
           SUM((CASE WHEN wait_category=15 THEN total_query_wait_time_ms ELSE 0 END)) AS network,               -- Async network IO, etc
           SUM((CASE WHEN wait_category=16 THEN total_query_wait_time_ms ELSE 0 END)) AS parallelism,           -- CXPACKET, CXCONSUMER
           SUM((CASE WHEN wait_category=17 THEN total_query_wait_time_ms ELSE 0 END)) AS memory,                -- Memory
           SUM((CASE WHEN wait_category=18 THEN total_query_wait_time_ms ELSE 0 END)) AS user_wait,             -- WAITFOR
           SUM((CASE WHEN wait_category=21 THEN total_query_wait_time_ms ELSE 0 END)) AS other_io               -- Async IO, backup IO, etc.
    FROM sys.query_store_wait_stats
    WHERE execution_type=0 -- success
    GROUP BY plan_id, runtime_stats_interval_id),

q AS (
    SELECT q.query_id,
           qp.plan_id,
           qt.query_sql_text,
           OBJECT_SCHEMA_NAME(q.[object_id])+N'.'+OBJECT_NAME(q.[object_id]) AS [object_name],
           (CASE WHEN q.[object_id]!=0 THEN q.last_compile_batch_offset_start ELSE 0 END) AS offset,
           x2.query_sql_text_no_params,
           ISNULL(x1.is_parameterized, 0) AS is_parameterized,
           q.query_parameterization_type_desc,
           qp.count_compiles AS compile_count,
           CAST(qrs.first_execution_time AS date) AS [date],

           SUM(qrs.count_executions) AS execution_count,
           CAST(.001*SUM(qrs.avg_duration*qrs.count_executions)/SUM(qrs.count_executions) AS numeric(20, 2)) AS avg_duration_ms,
           CAST(.001*SUM(qrs.avg_cpu_time*qrs.count_executions)/SUM(qrs.count_executions) AS numeric(20, 2)) AS avg_cpu_ms,
           CAST(SUM(qrs.avg_logical_io_reads*qrs.count_executions)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_reads,
           MAX(qrs.max_dop) AS max_dop,
           CAST(1.*MAX(qrs.max_query_max_used_memory)*8 AS numeric(20, 1)) AS avg_memory_grant_kb,
           CAST(1.*SUM(qrs.avg_rowcount*qrs.count_executions)/SUM(qrs.count_executions) AS numeric(20, 0)) AS avg_rowcount,
           CAST(1.*SUM(qrs.avg_log_bytes_used*qrs.count_executions)/SUM(qrs.count_executions)/1024 AS numeric(20, 0)) AS avg_log_kb,
           CAST(1.*SUM(qrs.avg_tempdb_space_used*qrs.count_executions)/SUM(qrs.count_executions)/8 AS numeric(20, 1)) AS avg_tempdb_kb,

           CAST(1.*SUM(w.sos_scheduler_yield)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_sos_scheduler_yield_wait_ms,
           CAST(1.*SUM(w.threadpool)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_threadpool_wait_ms,
           CAST(1.*SUM(w.lock)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_lock_wait_ms,
           CAST(1.*SUM(w.latch)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_latch_wait_ms,
           CAST(1.*SUM(w.pagelatch)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_pagelatch_wait_ms,
           CAST(1.*SUM(w.pageiolatch)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_pageiolatch_wait_ms,
           CAST(1.*SUM(w.compile)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_compile_wait_ms,
           CAST(1.*SUM(w.transaction_log)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_transaction_log_wait_ms,
           CAST(1.*SUM(w.network)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_network_wait_ms,
           CAST(1.*SUM(w.parallelism)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_parallelism_wait_ms,
           CAST(1.*SUM(w.memory)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_memory_wait_ms,
           CAST(1.*SUM(w.user_wait)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_user_wait_ms,
           CAST(1.*SUM(w.other_io)/SUM(qrs.count_executions) AS numeric(20, 1)) AS avg_other_io_wait_ms,

           CAST(qp.query_plan AS xml) AS query_plan
    FROM sys.query_store_query AS q
    INNER JOIN sys.query_store_query_text AS qt ON q.query_text_id=qt.query_text_id
    OUTER APPLY (
        SELECT PATINDEX('%)[^,)]%', qt.query_sql_text) AS offset, 1 AS is_parameterized
        WHERE qt.query_sql_text LIKE '(@%)%'
        ) AS x1
    CROSS APPLY (
        SELECT SUBSTRING(qt.query_sql_text, ISNULL(x1.offset, 0)+1, LEN(qt.query_sql_text)) AS query_sql_text_no_params
        ) AS x2
    LEFT JOIN sys.query_store_plan AS qp ON q.query_id=qp.query_id
    LEFT JOIN sys.query_store_runtime_stats AS qrs ON qp.plan_id=qrs.plan_id AND qrs.execution_type=0 -- success
    LEFT JOIN waits AS w ON qp.plan_id=w.plan_id AND qrs.runtime_stats_interval_id=w.runtime_stats_interval_id
    WHERE q.is_internal_query=0
 
    GROUP BY q.query_id,
           q.[object_id],
           qt.query_sql_text,
           x2.query_sql_text_no_params,
           qp.query_plan,
           q.last_compile_batch_offset_start,
           q.query_parameterization_type_desc,
           x1.is_parameterized,
           q.count_compiles,
           qp.plan_id,
           qp.count_compiles,
           CAST(qrs.first_execution_time AS date))

-------------------------------------------------------------------------------

SELECT ISNULL([object_name], N'') AS [Calling module],
       ISNULL(CAST(NULLIF(offset, 0) AS varchar(8)), '') AS [Module stmt],
       query_sql_text_no_params AS [Query, excl. parameters],
       (CASE is_parameterized WHEN 0 THEN '' ELSE 'Yes' END) AS [Parameterized],
       ISNULL(NULLIF(query_parameterization_type_desc, 'None'), '') AS [Param.type],
       compile_count AS [Compiles],
       [date] AS [Date],
       CONVERT(varchar(20), CAST(avg_duration_ms AS money), 1) AS [Avg duration, ms],
       CONVERT(varchar(20), CAST(avg_cpu_ms AS money), 1) AS [Avg cpu, ms],
       REPLACE(CONVERT(varchar(20), CAST(CAST(avg_reads AS bigint) AS money), 1), '.00', '') AS [Avg reads, pages],
       ISNULL(CAST(NULLIF(max_dop, 1) AS varchar(4)), '') AS [Max DOP],
       REPLACE(ISNULL(CONVERT(varchar(20), CAST(CAST(NULLIF(avg_memory_grant_kb, 0) AS bigint) AS money), 1), ''), '.00', '') AS [Avg mem.grant, kB],
       avg_rowcount AS [Avg rowcount],
       REPLACE(ISNULL(CONVERT(varchar(20), CAST(NULLIF(CAST(avg_log_kb AS bigint), 0) AS money), 1), ''), '.00', '') AS [Avg log, kB],
       REPLACE(ISNULL(CONVERT(varchar(20), CAST(NULLIF(CAST(avg_tempdb_kb AS bigint), 0) AS money), 1), ''), '.00', '') AS [Avg tempdb, kB],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_sos_scheduler_yield_wait_ms, 0) AS money), 1), '') AS [Avg cpu waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_threadpool_wait_ms, 0) AS money), 1), '') AS [Avg threadpool waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_lock_wait_ms, 0) AS money), 1), '') AS [Avg lock waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_latch_wait_ms, 0) AS money), 1), '') AS [Avg latch waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_pagelatch_wait_ms, 0) AS money), 1), '') AS [Avg page latch waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_pageiolatch_wait_ms, 0) AS money), 1), '') AS [Avg pageio latch waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_compile_wait_ms, 0) AS money), 1), '') AS [Avg compile waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_transaction_log_wait_ms, 0) AS money), 1), '') AS [Avg log waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_network_wait_ms, 0) AS money), 1), '') AS [Avg network waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_parallelism_wait_ms, 0) AS money), 1), '') AS [Avg parallelism waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_memory_wait_ms, 0) AS money), 1), '') AS [Avg memory waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_user_wait_ms, 0) AS money), 1), '') AS [Avg user waits, ms],
       ISNULL(CONVERT(varchar(20), CAST(NULLIF(avg_other_io_wait_ms, 0) AS money), 1), '') AS [Avg other IO waits, ms],
       query_plan AS [Plan]
FROM q

WHERE query_sql_text NOT LIKE '%sys.%'
  AND [date]>=DATEADD(day, -3, CAST(SYSDATETIME() AS date))

ORDER BY SUM(avg_cpu_ms) OVER (PARTITION BY query_id, plan_id) DESC, [date];
--ORDER BY [object_name], offset, [date]
