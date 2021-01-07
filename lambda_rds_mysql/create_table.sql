CREATE TABLE `sms_deliver_stats` (
	`message_id` VARCHAR(50) NOT NULL,
	`publish_date` VARCHAR(10) NOT NULL DEFAULT '',
	`phone_number` VARCHAR(50) ,
	`message_type` VARCHAR(50),
	`deliver_status` VARCHAR(50),
	`price_usd` DOUBLE,
	`part_number` INT(11),
	`total_parts` INT(11),
	`country` VARCHAR(128),
	PRIMARY KEY (`message_id`)
)
ENGINE=InnoDB;

