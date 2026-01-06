package com.jpmc.midascore.controller;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        // Use the same findById logic you used in the Kafka Consumer
        UserRecord user = userRepository.findById(userId).orElse(null);

        float balanceAmount = 0;
        if (user != null) {
            balanceAmount = user.getBalance();
        }

        return new Balance(balanceAmount);
    }
}
