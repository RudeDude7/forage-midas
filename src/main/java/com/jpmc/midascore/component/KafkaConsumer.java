package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
        // 1. Fetch Sender and Recipient from Database
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        // 2. Validate the Transaction
        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {

            // 3. Deduct from Sender, Add to Recipient
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            // 4. Save Updates to Database
            userRepository.save(sender);
            userRepository.save(recipient);

            // 5. Record the Transaction
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRecordRepository.save(record);

            logger.info("Transaction Success: {} sent {} to {}. New Sender Balance: {}",
                    sender.getName(), transaction.getAmount(), recipient.getName(), sender.getBalance());
        } else {
            logger.info("Transaction Failed: Invalid User or Insufficient Funds.");
        }
    }
}