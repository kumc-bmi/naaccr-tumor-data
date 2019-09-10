/** migrate_fact_upload -- append data from a workspace table.
*/

insert /*+ parallel(&&parallel_degree) append */ into &&I2B2STAR.observation_fact
select * from &&workspace_star.observation_fact_&&upload_id ;

insert into &&I2B2STAR.upload_status
select * from &&workspace_star.upload_status where upload_id = &&upload_id
-- Handle the case where workspace_star and I2B2STAR are the same.
and upload_id not in (select upload_id from &&I2B2STAR.upload_status)
;

update &&I2B2STAR.upload_status set load_status = 'OK'
where upload_id = &&upload_id;

commit ;


select 1 complete
from "&&I2B2STAR".upload_status
where upload_id = &&upload_id
;
