CREATE TABLE user_behavior
(
    `id`         BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `uid`        VARCHAR(100) NOT NULL,
    `time`       TIMESTAMP(3) NOT NULL,
    `phoneType`  VARCHAR(100) NOT NULL,
    `clickCount` INT(11)      NOT NULL,
    PRIMARY KEY (`id`)
)