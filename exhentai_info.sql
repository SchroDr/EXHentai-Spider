CREATE DATABASE exhentai;

use exhentai;

CREATE TABLE `exhentai_manga_info` (
    `id` varchar(30) NOT NULL,
    `head` varchar(500) DEFAULT NULL,
    `subhead` varchar(500) DEFAULT NULL,
    `kind` varchar(20) DEFAULT NULL,
    `uploader` varchar(30) DEFAULT NULL,
    `time` datetime DEFAULT NULL,
    `parent` varchar(30) DEFAULT NULL,
    `visible` varchar(10) DEFAULT NULL,
    `language` varchar(30) DEFAULT NULL,
    `file_size` float DEFAULT NULL,
    `length` int DEFAULT NULL,
    `favorited` int DEFAULT NULL,
    `rating_count` int DEFAULT NULL,
    `average_rating` float DEFAULT NULL,
    `features` varchar(1000) DEFAULT NULL,
    PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;