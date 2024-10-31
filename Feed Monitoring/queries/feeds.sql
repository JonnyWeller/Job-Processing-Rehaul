select ats.id as ats_id
, ats.name
, ats.type
, case
    when ats.id = 195 then 10449907  -- Public House Group
    when ats.id = 135 then 1560397  -- Andrew Brownsword Hotels
    when ats.id = 199 then 2632658  -- The Wave
    when ats.id = 9 then 3603364  -- homehouse
    when ats.id = 89 then 4901728  -- Various Eatieries
    when ats.id = 101 then 8641793  -- Pachamama Group
    when ats.id = 86 then 8768360  -- The Wolseley
    when ats.id = 69 then 6100314  -- Urban Leisure Group
    when ats.id = 141 then 1503033  -- Radisson
    when ats.id = 73 then 6345865  -- RedCat Pub Company
    when ats.id = 94 then 4718751  -- Liberation Group
    else ats.harri_brand_id
end as harri_brand_id
, v.id as placed_account_id
, case
    when ats.name like "ETM%" or ats.name like "Maven%" then "https://etm.ats.emea1.fourth.com/services/api/vacancies/placed-all-brands"
    when ats.name like "The Alchemist%" then "https://thealchemist.ats.emea1.fourth.com/services/api/vacancies/placed-all-brands"
    when ats.name like "Sky%" then "https://batchaws.adcourier.com/services/?q=U2FsdGVkX19HQHPTpm-Vr0dqy4RkMG7UlrCOaiDSzi7Jr_CZzcnYu21qUHOvJ4S6"
    when ats.name like "Horizon%" then "https://batchaws.adcourier.com/services/?q=U2FsdGVkX19HQHPTpm-Vr0dqy4RkMG7UlrCOaiDSzi7Jr_CZzcnYu21qUHOvJ4S6"
    when ats.name like "IHG%" then "https://xml.applygateway.com/CPABidding/xml_live/PlacedApp-49739797.xml"
    when ats.name like "Barchester%" then "https://xml.applygateway.com/CPABidding/xml_live/PlacedApp-49739797.xml"
    when ats.name like "Uber%" then "https://joveo-outbound-feeds-prod.s3-accelerate.amazonaws.com/joveo-a256b382/0a9712f5.xml"
    when ats.name like "Lidl%" then "https://careers.lidl.co.uk/feed/jobfeed?client=uk&language=en"
    when ats.name like "Co-op%" then "https://placedappgbp:kMEihKlVcbwOihbn@job-feed.perengo.com/placedappgbp/perengo_placedappgbp_job_feed.xml.gz"
    when ats.name like "Primark%" then "https://placedappgbp:kMEihKlVcbwOihbn@job-feed.perengo.com/placedappgbp/perengo_placedappgbp_job_feed.xml.gz"
    when ats.name like "Virgin Money%" then "https://placedappgbp:kMEihKlVcbwOihbn@job-feed.perengo.com/placedappgbp/perengo_placedappgbp_job_feed.xml.gz"
    when ats.name like "Capital One%" then "https://placedappgbp:kMEihKlVcbwOihbn@job-feed.perengo.com/placedappgbp/perengo_placedappgbp_job_feed.xml.gz"
    when ats.name like "Direct Line Group%" then "https://placedappgbp:kMEihKlVcbwOihbn@job-feed.perengo.com/placedappgbp/perengo_placedappgbp_job_feed.xml.gz"
    when ats.name like "Exchange (Organic)%" then "https://exchangefeeds.s3.amazonaws.com/45a73d07dbee2f62dc6d46489fd87005/feed.xml.gz"
    when ats.name like "HCA%" then "https://wd3-services1.myworkday.com/ccx/service/customreport2/hcahealthcare/CR26-ISU/CR_26_Workday_to_placed-app-Job_Postings?format=simplexml"
    when ats.name like "Brandon Trust%" then "https://brandontrust.ciphr-irecruit.com/rssfeed.aspx"
    when ats.name like "PizzaExpress%" then "https://genius-api.ats.careers/api/56860AF6-9256-4C10-B898-63502831F73E/feed/jobboard/placed"
    when ats.name like "Evri%" then "https://clickcastfeeds.s3.amazonaws.com/ad4f90ddfcaaaf7a40abf666e2a2e3c6/5be7f721520478cb8f228924b0692720.xml.gz"
    when ats.name like "Aviva%" then "https://joveo-c08b42a8.s3-accelerate.amazonaws.com/bc13de09.xml"
    -- when ats.name like "Cycas%" then "https://talos360.com/feeds/jobboardonemail10"
    else ats.fetch_url
end as fetch_url
, if(ats.fetch_url regexp "https://apply.placed-app.com/\.*", ats.fetch_url, null) as middleware_url
, ats.fetch_url regexp "https://apply.placed-app.com/\.*" as is_middleware
, if(v.id is null, 0, 1) as is_linked_to_venue
, case
    when ats.id in (142, 85) then 1 else 0
end as requires_password
, case
    when ats.id in (224, 225, 226, 227, 228, 229, 116, 117, 95, 166, 184, 185, 181, 180, 237, 243, 248, 247) then 1 else 0
end as is_json
, case
    when ats.id in (184, 185, 181, 180, 248) then "JobID"
    when ats.id in (95, 243) then "JobId"
    when ats.id in (166, 224, 225, 226, 227, 228, 229, 116, 117, 237, 247) then "title"
    else null
end as target_key
, case
    when ats.id in (224, 225, 226, 227, 228, 229, 116, 117, 95, 237, 243, 247) then 1
    when ats.id in (166, 184, 185, 181, 180, 248) then 2
    else null
end as json_max_depth
, case
    when ats.id in (79, 94, 113, 164, 198, 138, 159, 202, 187, 200, 222, 192, 183,
    175, 174, 114, 221, 201, 158, 204, 203, 206, 207, 96, 9, 69, 86, 101, 131, 135,
    141, 199, 104, 133, 223, 73, 195, 234, 235, 238, 240, 249, 250, 251, 253, 254,
    236, 255, 256, 245)
    then "job"
    when ats.id in (167) then "item"
    when ats.id in (193, 172) then "Jobs"
    when ats.id in (182) then "Item"
    when ats.id in (142) then "wd:Report_Entry"
    when ats.id in (177, 178, 179, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218,
    219, 232, 233) then "joboffer"
end as target_xml_element
, header_api_token
from ats
left join venue v on v.ats_id = ats.id
where ats.is_active = 1
and v.id is not null
and ats.id not in (105, 107, 119, 120, 121, 122)  # Sessions (TeamTailor), Unused IHG Feeds
group by ats.id
order by ats.name, ats.type, ats.fetch_url