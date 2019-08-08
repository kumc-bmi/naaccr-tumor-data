## GENERATED_SQL from GPC Breast Cancer query

We have 2 i2b2 queries from the GPC Breast Cancer Survey:

 - [bc204_query_definition.xml][204x]
   - i2b2 query for underlying breast cancer study population [#204]
 - [bc295_query_definition.xml][295x]
   - breast cancer correspondence between EHR and tumor registry variables - i2b2 query [#295][]
 
[204]: https://informatics.gpcnetwork.org/trac/Project/ticket/204
[295]: https://informatics.gpcnetwork.org/trac/Project/ticket/295 

[204x]: https://github.com/kumc-bmi/bc_qa/blob/master/bc204_query_definition.xml
[295x]: https://github.com/kumc-bmi/bc_qa/blob/rc_codebook/bc2_collect/bc295_query_definition.xml

We get the `GENERATED_SQL` from follows; from babel:

```
select query_master_id, create_date generated_sql from qt_query_master
where name = 'Bre-30 -044-Lan@10:47:06'

349	2015-02-25 16:46:59.976
```

and from heron:

```
select query_master_id, create_date, generated_sql from blueherondata.qt_query_master
where name = 'BC Phase 2g (exported)@13:23:47'
```


## Synthetic NAACCR data (WIP)

See tr_summary.py
