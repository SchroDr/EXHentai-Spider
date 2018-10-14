CREATE DATABASE exhentai;

use exhentai;

CREATE TABLE `exhentai_info` (
    `manga_pure_id` varchar(30) NOT NULL,
    `manga_id` varchar(30) NOT NULL,
    `head` varchar(500) DEFAULT NULL,
    `subhead` varchar(500) DEFAULT NULL,
    `kind` varchar(20) DEFAULT NULL,
    `uploader` varchar(30) DEFAULT NULL,
    `time` datetime DEFAULT NULL,
    `parent` varchar(30) DEFAULT NULL,
    `parent_href` varchar(30) DEFAULT NULL,
    `visible` varchar(10) DEFAULT NULL,
    `language` varchar(30) DEFAULT NULL,
    `file_size` float DEFAULT NULL,
    `length` int DEFAULT NULL,
    `favorited` int DEFAULT NULL,
    `rating_count` int DEFAULT NULL,
    `average_rating` float DEFAULT NULL,
    `artist_feature` varchar(500) DEFAULT NULL,
    `group_feature` varchar(500) DEFAULT NULL,
    `female_feature` varchar(500) DEFAULT NULL,
    `male_feature` varchar(500) DEFAULT NULL,
    `language_feature` varchar(500) DEFAULT NULL,
    `character_feature` varchar(500) DEFAULT NULL,
    `misc_feature` varchar(500) DEFAULT NULL,
    `parody_feature` varchar(500) DEFAULT NULL,
    PRIMARY KEY (`manga_pure_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;