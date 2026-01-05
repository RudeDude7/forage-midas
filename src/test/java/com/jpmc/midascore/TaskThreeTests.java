package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.UserRecord;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(2000);


        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to find out what waldorf's balance is after all transactions are processed");
        logger.info("kill this test once you find the answer");
        while (true) {
            Thread.sleep(5000); // Check every 5 seconds
            logger.info("Checking database...");

            // Debug 1: Check if the database is empty
            long count = userRepository.count();
            logger.info("Total Users in Database: " + count);

            if (count == 0) {
                logger.info("!!! Database is empty. Kafka Consumer might not be working !!!");
                continue;
            }

            // Debug 2: Print the first few names to check casing
            if (count > 0) {
                logger.info("Sample user: " + userRepository.findAll().iterator().next().getName());
            }

            // Debug 3: Try to find Waldorf (Capitalized and Lowercase)
            com.jpmc.midascore.entity.UserRecord waldorf = userRepository.findByName("waldorf");
            if (waldorf == null) {
                waldorf = userRepository.findByName("Waldorf"); // Try Capitalized
            }

            if (waldorf != null) {
                logger.info("----------------------------------------------------------");
                logger.info("YOUR ANSWER (WALDORF BALANCE): " + waldorf.getBalance());
                logger.info("----------------------------------------------------------");
                break; // Stop the loop once found
            } else {
                logger.info("User 'waldorf' (or Waldorf) still not found...");
            }
        }
    }
}
