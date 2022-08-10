
select
    cntrycode,
    count(*) as numcust,
    to_int64(sum(c_acctbal)) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            customer
        where
                substring(c_phone from 1 for 2) in
                ('13', '31', '23', '29', '30', '18', '17')
          and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                    c_acctbal > 0.00
              and substring(c_phone from 1 for 2) in
                  ('13', '31', '23', '29', '30', '18', '17')
        )
          and not exists (
                select
                    *
                from
                    orders
                where
                        o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode;



drop table customer;
drop table orders;
drop table lineitem;
drop table nation;
drop table region;
drop table supplier;
drop table part;
drop table partsupp;
set enable_planner_v2 = 0;
