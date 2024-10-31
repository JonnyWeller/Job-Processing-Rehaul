with ats_venues as (
select cv.venue_id
, rm.ats_id
, rm.client_name as ats_name
from landing.robotan_mapping rm
left join dashboards.clean_venue cv on cv.ats_id = rm.ats_id
where rm.ats_id <> 0
group by 1
)
, programmatic_jobs_settings as (
select venue_id
, count(distinct job_offer_id) as programmatic_live_jobs
from programmatic.live_jobs_new
where promotion_active = 1
and is_active = 1
group by 1
)
, live_jobs_settings as (
select venue_id
, count(distinct lj.job_offer_id) as landing_live_jobs
, count(distinct case when lj.boosted = 1 then lj.job_offer_id end) as landing_live_jobs_boosted
from landing.live_jobs lj
group by 1
)
select av.ats_id
, av.ats_name
, ifnull(sum(landing_live_jobs), 0) as landing_live_jobs
, ifnull(sum(landing_live_jobs_boosted), 0) as landing_live_jobs_boosted
, ifnull(sum(pjs.programmatic_live_jobs), 0) as programmatic_live_jobs
from ats_venues av
left join programmatic_jobs_settings pjs on pjs.venue_id = av.venue_id
left join live_jobs_settings ljs on ljs.venue_id = av.venue_id
where av.venue_id is not null
group by 1, 2