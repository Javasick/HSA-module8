package com.projector;

import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;

@SpringBootApplication
public class Application implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static final Faker FAKER = new Faker();
    public static final int BATCH_SIZE = 1_000;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) {
        createTable();
        log.info("Table created");
        insertData();
        log.info("Records count: {}", jdbcTemplate.queryForObject("SELECT COUNT(*) FROM `user`", Long.class));
        selectByBirthDate();
        createIndex();
        log.info("Index by birth date created");
        selectByBirthDate();
        log.info("Bye");
    }

    private void createTable() {
        jdbcTemplate.execute("" +
                "CREATE TABLE `user` (" +
                "  `id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                "  `name` varchar(64) NOT NULL DEFAULT ''," +
                "  `email` varchar(64) NOT NULL DEFAULT ''," +
                "  `birth_date` DATE, " +
                "  PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci");
    }

    private void insertData() {
        String insertQuery = "INSERT INTO `user` (name, email, birth_date) VALUES (?, ?, ?);";
        log.info("Data generation started");
        for (int i = 0; i < 40_000; i++) {
            jdbcTemplate.batchUpdate(insertQuery, getTenKUsers());
            log.info("{} users created", (i + 1) * BATCH_SIZE);
        }
        log.info("Fake users created");
    }

    private List<Object[]> getTenKUsers() {
        List<Object[]> result = new LinkedList<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            result.add(new Object[]{FAKER.name().name(), FAKER.internet().emailAddress(), FAKER.date().birthday()});
        }
        return result;
    }

    private void selectByBirthDate() {
        Instant start = Instant.now();
        jdbcTemplate.queryForObject("SELECT COUNT(*) FROM `user` WHERE birth_date = '1984-01-01'", Long.class);
        Instant end = Instant.now();
        log.info("Query time = {}", Duration.between(start, end).toMillis());
    }

    private void createIndex() {
        jdbcTemplate.execute("ALTER TABLE `user` ADD INDEX `idx_birth_date` (`birth_date` ASC)");
    }
}
