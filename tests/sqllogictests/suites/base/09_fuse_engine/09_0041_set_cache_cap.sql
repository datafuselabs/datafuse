statement ok
create or replace database db_09_0041;

statement ok
use db_09_0041;

# By default, memory_cache_block_meta is disabled,
# let's enable it by setting a non-zero capacity
statement ok
call system$set_cache_capacity('memory_cache_block_meta', 1000);

# check cache "memory_cache_block_meta" exists

query II
select count()>=1 from system.caches where name = 'memory_cache_block_meta' and capacity = 1000;
----
1
