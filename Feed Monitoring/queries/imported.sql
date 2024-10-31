select ats.id as ats_id
, ats.name as ats_name
, ats.type as ats_type
, count(job_offer_imported.id) as job_offer_imported_jobs
, count(case when job_offer_imported.date_auto_updated is not null then job_offer_imported.id end) as robotan_attempted
, count(distinct job_offer.id) as placed_app_jobs
, count(case when job_offer.is_active then job_offer.id end) as placed_app_jobs_live
, max(ifnull(job_offer_imported.synced_at, job_offer.updated_at)) as last_synced_date
from job_offer
left join job_offer_imported on job_offer_imported.job_offer_id = job_offer.id
left join ats on ats.id = if(job_offer.ats_id is null, job_offer_imported.ats_id, job_offer.ats_id)
where (job_offer_imported.on_feed = 1 or job_offer.ats_id is not null)
and ats.is_active = 1
and ats.id not in (84, 111)
and job_offer.deletedAt is null
and job_offer.venue_id not in (13295, 4081, 13568, 13628)  # excludes test, my four wheels, appcast2
group by 1
order by 4 desc, 1 asc