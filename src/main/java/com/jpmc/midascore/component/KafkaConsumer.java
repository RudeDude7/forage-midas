package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @Autowired
    private RestTemplate restTemplate;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {

            // 1. Call the Incentive API
            Incentive incentive = restTemplate.postForObject(
                    "http://localhost:8080/incentive",
                    transaction,
                    Incentive.class
            );

            float incentiveAmount = incentive.getAmount();

            // 2. Adjust balances (Sender loses amount; Recipient gets amount + incentive)
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);

            // 3. Save the Transaction with the Incentive
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
            record.setIncentive(incentiveAmount); // Store the incentive
            transactionRecordRepository.save(record);

            logger.info("Processed: {} -> {} | Amount: {} | Incentive: {}",
                    sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount);
        } else {
            logger.info("Transaction rejected.");
        }
    }
}