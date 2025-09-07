package com.jpmc.midascore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private RestTemplate restTemplate;

    @KafkaListener(topics = "${general.kafka-topic}", groupId="midas-core-group")
    @Transactional
    public void consumeTransaction(String message) throws Exception{
        Transaction transaction = objectMapper.readValue(message, Transaction.class);
        System.out.println("Received transaction: " + transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {

            String incentiveApiUrl = "http://localhost:8080/incentive";
            Incentive incentive = restTemplate.postForObject(incentiveApiUrl, transaction, Incentive.class);
            float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0;

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);

            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
            transactionRepository.save(transactionRecord);
        }
    }
}