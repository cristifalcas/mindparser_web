drop database mind_statistics;
create database mind_statistics;
grant index, create, select, insert, update, delete, drop, alter, lock tables on  mind_statistics.* to 'mind_statistics'@'%' identified by '!0mind_statistics@9';
use mind_statistics;

CREATE TABLE IF NOT EXISTS customers (
	id int not null AUTO_INCREMENT,
	name varchar(50) UNIQUE,
	PRIMARY KEY(id)
	);

CREATE TABLE IF NOT EXISTS hosts (
	id int not null AUTO_INCREMENT,
	customer_id int,
	name varchar(50),
	ip varchar(50),
	UNIQUE idx_cust_name (customer_id, name),
	PRIMARY KEY(id),
	FOREIGN KEY (customer_id) REFERENCES customers(id)
	);
 
CREATE TABLE IF NOT EXISTS collected_files (
	id int not null AUTO_INCREMENT,
	customer_id int not null,
	host_id int not null,
	file_name varchar(2500) not null,
	file_md5 varchar(50) not null,
	size int,
	inserttime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	parse_duration decimal(15,5),
	parse_done_time DATETIME,
	inserted_in_tablename varchar(100),
	app_name varchar(100),
	worker_type varchar(100),
	status int not null,
	PRIMARY KEY(id)
	);

CREATE TABLE IF NOT EXISTS statistics_template (
	id int not null AUTO_INCREMENT,
	file_id int not null,
	host_id int not null,
	timestamp int not null,
	date varchar(20) not null,
	time varchar(20) not null,
	PRIMARY KEY(id),
	UNIQUE idx_hid_ts (host_id, timestamp),
	FOREIGN KEY (host_id) REFERENCES hosts(id)
	);

-- CREATE TABLE IF NOT EXISTS rtsstatistics LIKE statistics_template; 
-- ALTER TABLE rtsstatistics add FOREIGN KEY (host_id) REFERENCES hosts(id);

CREATE TABLE IF NOT EXISTS md5_col_names (
	md5 varchar(50) not null,
	name varchar(200) not null,
	UNIQUE idx_md5(md5),
	UNIQUE idx_name(name)
	);
