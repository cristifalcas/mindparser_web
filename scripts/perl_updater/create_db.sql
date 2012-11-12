drop database mind_statistics;
create database mind_statistics;
grant index, create, select, insert, update, delete, drop, alter, lock tables on  mind_statistics.* to 'mind_statistics'@'%' identified by '!0mind_statistics@9';
use mind_statistics;

CREATE TABLE IF NOT EXISTS __customers (
	id int not null AUTO_INCREMENT,
	name varchar(50) UNIQUE,
	PRIMARY KEY(id)
	);
INSERT INTO __customers (id, name) VALUES (-1, '__deleted__');

CREATE TABLE IF NOT EXISTS __hosts (
	id int not null AUTO_INCREMENT,
	customer_id int,
	name varchar(50),
	ip varchar(50),
	UNIQUE idx_cust_name (customer_id, name),
	PRIMARY KEY(id),
	FOREIGN KEY (customer_id) REFERENCES __customers(id)
	);
INSERT INTO __hosts (id, customer_id, name) VALUES (-1, -1, '__deleted__');
 
CREATE TABLE IF NOT EXISTS __mind_plugins (
	id int not null AUTO_INCREMENT,
	customer_id int not null,
	host_id int not null,
	inserted_in_tablename varchar(100),
	worker_type varchar(100),
	app_name varchar(100),
	plugin_name varchar(100),
	files_queue int,
	update_rate int not null,
	needs_update bool,
	FOREIGN KEY (customer_id) REFERENCES __customers(id),
	FOREIGN KEY (host_id) REFERENCES __hosts(id),
	UNIQUE idx_mind_plugins (customer_id, host_id, worker_type, plugin_name),
	PRIMARY KEY(id)
	);
INSERT INTO __mind_plugins (id, customer_id, host_id) VALUES (-1, -1, -1);

CREATE TABLE IF NOT EXISTS __collected_files (
	id int not null AUTO_INCREMENT,
	customer_id int not null,
	host_id int not null,
	plugin_id int not null,
	file_name varchar(2500) not null,
	file_md5 varchar(50) not null,
	size int,
	inserttime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	parse_duration decimal(15,5),
	parse_done_time DATETIME,
	status int not null,
	FOREIGN KEY (customer_id) REFERENCES __customers(id),
	FOREIGN KEY (host_id) REFERENCES __hosts(id),
	FOREIGN KEY (plugin_id) REFERENCES __mind_plugins(id),
	INDEX idx_status (status),
	PRIMARY KEY(id)
	);
INSERT INTO __collected_files (id, customer_id, host_id, plugin_id, status) VALUES (-1, -1, -1, -1, -1);

CREATE TABLE IF NOT EXISTS __md5_col_names (
	md5 varchar(50) not null,
	name varchar(200) not null,
	UNIQUE idx_md5(md5),
	UNIQUE idx_name(name)
	);
INSERT INTO __md5_col_names (md5, name) VALUES ('__munin_extra_info', '__munin_extra_info');

CREATE TABLE IF NOT EXISTS __mind_plugins_conf (
	plugin_id int not null,
	section_name varchar(50) not null,
	md5_name varchar(50) not null,
	extra_info varchar(2500) not null,
	FOREIGN KEY (plugin_id) REFERENCES __mind_plugins(id),
	FOREIGN KEY (md5_name) REFERENCES __md5_col_names(md5),
	UNIQUE idx_mind_plugins_conf (plugin_id, md5_name)
	);

-- CREATE TABLE IF NOT EXISTS __statistics_template (
-- 	file_id int not null,
-- 	host_id int not null,
-- 	timestamp int not null,
-- 	group_by varchar(20),
-- 	UNIQUE idx_hid_ts (host_id, timestamp, group_by),
-- 	FOREIGN KEY (host_id) REFERENCES __hosts(id)
-- 	);

-- CREATE TABLE IF NOT EXISTS rtsstatistics LIKE statistics_template; 
-- ALTER TABLE rtsstatistics add FOREIGN KEY (host_id) REFERENCES hosts(id);

