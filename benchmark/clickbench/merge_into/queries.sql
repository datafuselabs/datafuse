-- test update and delete, insert
set enable_experimental_merge_into = 1;merge into target_table as t1 using (select * from source_table as t2)  on t1.l_partkey = t2.l_partkey and t1.l_orderkey = t2.l_orderkey and t1.l_suppkey = t2.l_suppkey and t1.l_linenumber = t2.l_linenumber when matched and t1.l_partkey >= 200000 then update * when matched then delete when not matched then insert *;
