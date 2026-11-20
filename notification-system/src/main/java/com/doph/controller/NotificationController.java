package com.doph.controller;

import com.doph.dto.PrivateMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.security.Principal;
import java.util.Random;

@Controller
public class NotificationController {

    private static final Random random = new Random();

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    /*
    /app/send-message
    */
    @MessageMapping("/send-message")
    @SendTo("/topic/notifications")
    public String sendPublicMessage(String message, Principal sender) {
        String senderName = (sender != null) ? sender.getName() : "Unknown";
        double randomNumber = Math.round(random.nextDouble(200) * 10000) * 1.0 / 10000;
        return "[PUBLIC - " + senderName + "]: " + message + " - " + randomNumber;
    }


    /*
    /app/send-private
    */
    @MessageMapping("/send-private")
    public void sendPrivateMessage(PrivateMessage message, Principal sender){
        String senderName = (sender != null) ? sender.getName() : "Unknow";
        double randomNumber = Math.round(random.nextDouble(200) * 10000) * 1.0 / 10000;
        String payload = "From [" + senderName + "]: " + message.content() + " - " + randomNumber;
        messagingTemplate.convertAndSendToUser(
                message.toUser(),
                "/queue/notifications",
                payload
        );
    }
}
