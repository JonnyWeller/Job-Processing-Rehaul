CREATE TABLE insights.feed_monitoring
(id INT auto_increment NOT NULL,
ats_id INT NOT NULL,
ats_name VARCHAR(255) NOT NULL,
source_feed_jobs INT NOT NULL,
middleware_feed_jobs INT NOT NULL,
job_offer_imported_jobs INT NOT NULL,
robotan_attempted INT NOT NULL,
placed_app_jobs INT NOT NULL,
placed_app_jobs_live INT NOT NULL,
updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (id),
KEY (ats_id)
)