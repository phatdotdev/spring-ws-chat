# Hướng dẫn triển khai & Bảo mật WebSocket (STOMP) trong Spring Boot

Tài liệu tổng hợp toàn diện cách thiết lập giao tiếp hai chiều thời gian thực (Real-time) bằng **WebSocket + STOMP** trên Spring Boot, bao gồm luồng kết nối, gửi/nhận tin nhắn (Broadcast & Private) và kiến trúc bảo mật thực tế với **Session/Cookie** lẫn **JWT**.

---

## 1. Kiến trúc & Luồng hoạt động tổng quan

Bản thân WebSocket chỉ là một đường ống truyền dữ liệu thô. Spring Boot kết hợp thêm giao thức **STOMP** (Simple Text Oriented Messaging Protocol) để chuẩn hóa cách định tuyến tin nhắn thông qua các tiền tố (prefix):

```text
[Client / Browser]
   │
   ├── 1. HTTP Handshake (GET /ws) ──► [Spring Security Filter Chain] (Kiểm tra Cookie / CORS)
   │                                           │ (Nâng cấp lên giao thức WebSocket - HTTP 101)
   ▼                                           ▼
[Kết nối WebSocket bền vững] ◄──────► [Client Inbound Channel] ◄── [ChannelInterceptor] (Kiểm tra JWT / Quyền)
                                               │
                 ┌─────────────────────────────┴─────────────────────────────┐
                 ▼ (Gửi lên: /app/...)                                       ▼ (Đăng ký: /topic/... hoặc /user/queue/...)
        [@MessageMapping Controller]                                  [Simple Message Broker]
                 │                                                           ▲
                 └─── @SendTo hoặc SimpMessagingTemplate.convertAndSend* ────┘
```

### Ý nghĩa các tiền tố đường dẫn (Destination Prefixes)
* **`/ws`**: Endpoint dùng để thiết lập kết nối ban đầu (Handshake).
* **`/app/...`**: Tin nhắn từ Client gửi lên Server để xử lý logic nghiệp vụ (được ánh xạ vào `@MessageMapping`).
* **`/topic/...`**: Kênh công khai (**Publish-Subscribe / Broadcast**). Server đẩy tin vào đây, tất cả Client đang subscribe đều nhận được.
* **`/queue/...`** kết hợp **`/user/...`**: Kênh riêng tư (**Point-to-Point / Unicast**). Server đẩy tin tới đích danh 1 người dùng cụ thể.

---

## 2. Cài đặt Dependencies (`pom.xml`)

```xml
<!-- WebSocket & STOMP -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-websocket</artifactId>
</dependency>

<!-- Spring Security (Dùng cho phần bảo mật) -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-security</artifactId>
</dependency>
```

---

## 3. Cách Kết nối, Gửi tin nhắn tới & Đẩy tin nhắn đi

### 3.1. Cấu hình Message Broker (`WebSocketConfig.java`)

```java
package com.example.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        // Kích hoạt bộ định tuyến trong bộ nhớ cho kênh chung (/topic) và kênh riêng (/queue)
        config.enableSimpleBroker("/topic", "/queue");

        // Tiền tố cho các request từ Client gửi lên Controller
        config.setApplicationDestinationPrefixes("/app");

        // Tiền tố định danh cho tin nhắn gửi riêng từng người dùng
        config.setUserDestinationPrefix("/user");
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("http://localhost:8080", "http://localhost:3000")
                .withSockJS(); // Fallback sang HTTP Streaming/Polling nếu trình duyệt chặn WS
    }
}
```

### 3.2. Nhận tin nhắn từ Client & Đẩy tin nhắn đi (`ChatController.java`)

```java
package com.example.controller;

import com.example.dto.ChatMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.security.Principal;

@Controller
public class ChatController {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    // CÁCH 1: Nhận từ /app/chat.public và đẩy cho TẤT CẢ mọi người (/topic/public)
    @MessageMapping("/chat.public")
    @SendTo("/topic/public")
    public String sendPublicMessage(String content, Principal sender) {
        String username = (sender != null) ? sender.getName() : "Anonymous";
        return username + ": " + content;
    }

    // CÁCH 2: Nhận từ /app/chat.private và đẩy cho ĐÚNG 1 NGƯỜI (/user/{toUser}/queue/messages)
    @MessageMapping("/chat.private")
    public void sendPrivateMessage(ChatMessage message, Principal sender) {
        String fromUser = sender.getName(); // Luôn lấy danh tính từ Server, không tin dữ liệu client gửi
        String payload = "[Riêng từ " + fromUser + "]: " + message.getContent();

        // Đẩy tới người nhận (Spring tự dịch thành /user/{toUser}/queue/messages)
        messagingTemplate.convertAndSendToUser(
                message.getToUser(),
                "/queue/messages",
                payload
        );
    }

    // CÁCH 3: Đẩy thông báo chủ động từ bất kỳ đâu (Service, REST API, Cronjob...)
    public void pushSystemNotification(String targetUsername, String alertText) {
        messagingTemplate.convertAndSendToUser(
                targetUsername,
                "/queue/notifications",
                alertText
        );
    }
}
```

### 3.3. Kết nối, Gửi & Nhận ở phía Client (JavaScript)

```javascript
const socket = new SockJS('/ws');
const stompClient = Stomp.over(socket);

// 1. Kết nối tới Server
stompClient.connect({}, function (frame) {
    console.log('Đã kết nối:', frame);

    // 2. Đăng ký nhận tin nhắn CHUNG
    stompClient.subscribe('/topic/public', function (response) {
        console.log('Tin chung:', response.body);
    });

    // 3. Đăng ký nhận tin nhắn RIÊNG (Spring tự ánh xạ /user với user đang đăng nhập)
    stompClient.subscribe('/user/queue/messages', function (response) {
        console.log('Tin riêng:', response.body);
    });
});

// 4. Gửi tin nhắn chung lên Server
function sendPublic(text) {
    stompClient.send('/app/chat.public', {}, text);
}

// 5. Gửi tin nhắn riêng lên Server
function sendPrivate(toUser, text) {
    stompClient.send('/app/chat.private', {}, JSON.stringify({
        toUser: toUser,
        content: text
    }));
}
```

---

## 4. Cơ chế Bảo mật: Spring Security xử lý thế nào và chuyển sang WebSocket ra sao?

Hiểu rõ ranh giới giữa HTTP và WebSocket là chìa khóa để bảo mật đúng cách:

### Giai đoạn 1: HTTP Handshake (Đi qua Spring Security Filter Chain)
Khi Client gọi `new SockJS('/ws')`, trình duyệt gửi một HTTP request thông thường (`GET /ws/info`). Request này **đi qua toàn bộ chuỗi Filter của Spring Security** (`UsernamePasswordAuthenticationFilter`, `JwtAuthenticationFilter`, `CsrfFilter`, `CorsFilter`...).
* Nếu dùng **Session/Cookie**: Spring Security đọc `JSESSIONID`, khôi phục `SecurityContext` và gán sẵn đối tượng `Principal` vào `HttpServletRequest`. Khi nâng cấp kết nối lên WebSocket (HTTP `101 Switching Protocols`), Spring WebSocket **tự động kế thừa `Principal` này**.
* Nếu dùng **JWT (Stateless)**: Trình duyệt **không thể** tự gắn header `Authorization: Bearer <token>` vào kết nối WebSocket thuần hoặc SockJS lúc Handshake. Do đó, endpoint `/ws/**` thường được mở ở tầng HTTP (`permitAll()`), và việc xác thực JWT được đẩy sang **Giai đoạn 2**.

### Giai đoạn 2: Truyền gói tin STOMP (Đi qua `ClientInboundChannel`)
Sau khi nâng cấp thành công lên kết nối TCP bền vững, các lệnh tiếp theo (`CONNECT`, `SUBSCRIBE`, `SEND`, `DISCONNECT`) là các khung dữ liệu STOMP, **không còn là HTTP request** nên sẽ **KHÔNG đi qua Spring Security Filter Chain nữa**.
Thay vào đó, mọi gói tin từ Client gửi vào đều phải đi qua **`clientInboundChannel`**. Tại đây, ta đặt một **`ChannelInterceptor`** để kiểm tra danh tính và phân quyền.

### 4 Chốt chặn bảo mật bắt buộc
1. **Chốt chặn 1 — Origin / CORS (`setAllowedOriginPatterns`):** Ngăn chặn trang web độc hại khác mở kết nối WebSocket ké cookie của người dùng (lỗi *Cross-Site WebSocket Hijacking - CSWSH*).
2. **Chốt chặn 2 — HTTP Security (`SecurityFilterChain`):** Kiểm soát quyền truy cập `/ws/**` và bảo vệ CSRF (đối với Cookie).
3. **Chốt chặn 3 — STOMP Interceptor (`ChannelInterceptor`):**
   * Tại lệnh `CONNECT`: Xác thực JWT (nếu dùng token) hoặc từ chối nếu `Principal == null`.
   * Tại lệnh `SUBSCRIBE`: Ngăn người dùng A tự ý subscribe vào kênh riêng `/user/userB/queue/messages` để nghe lén.
4. **Chốt chặn 4 — Business Logic (`@MessageMapping`):** Lấy tên người gửi từ tham số `Principal` do Server nắm giữ, tuyệt đối không tin trường `fromUser` trong JSON do Client gửi lên.

---

## 5. Ví dụ 1: Bảo mật bằng Session & Cookie (Stateful)

Phù hợp cho ứng dụng render giao diện từ Server (Thymeleaf) hoặc chung domain, sử dụng `HttpSession` + Cookie `JSESSIONID`.

### Bước 1: Cấu hình Spring Security (`SecurityConfig.java`)
Khi dùng Cookie, trình duyệt tự động đính kèm `JSESSIONID` lúc gọi `/ws`, nên Spring Security tự nhận diện được user. Đồng thời mặc định Spring Security bật CSRF, bạn cần giữ CSRF để chống tấn công CSWSH.

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/login", "/css/**", "/js/**").permitAll()
                .requestMatchers("/ws/**").authenticated() // Bắt buộc đăng nhập mới được Handshake
                .anyRequest().authenticated()
            )
            .formLogin(Customizer.withDefaults());
        return http.build();
    }
}
```

### Bước 2: Chặn nghe lén kênh riêng trong `WebSocketConfig.java`

```java
@Override
public void configureClientInboundChannel(ChannelRegistration registration) {
    registration.interceptors(new ChannelInterceptor() {
        @Override
        public Message<?> preSend(Message<?> message, MessageChannel channel) {
            StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);

            if (accessor != null) {
                Principal user = accessor.getUser(); // Tự động lấy từ HttpSession của Spring Security

                // 1. Từ chối nếu chưa đăng nhập
                if (StompCommand.CONNECT.equals(accessor.getCommand()) && user == null) {
                    throw new AccessDeniedException("Chưa đăng nhập!");
                }

                // 2. Chống nghe lén kênh riêng của người khác
                if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
                    String destination = accessor.getDestination();
                    if (destination != null && destination.startsWith("/user/")) {
                        String targetUser = destination.split("/")[2];
                        if (user == null || !user.getName().equals(targetUser)) {
                            throw new AccessDeniedException("Không được phép nghe kênh của " + targetUser);
                        }
                    }
                }
            }
            return message;
        }
    });
}
```

### Bước 3: Truyền CSRF Token ở Frontend (Thymeleaf)

```html
<!-- Đặt trong thẻ <head> của Thymeleaf -->
<meta name="_csrf" th:content="${_csrf.token}"/>
<meta name="_csrf_header" th:content="${_csrf.headerName}"/>

<script>
    const csrfHeader = document.querySelector('meta[name="_csrf_header"]').content;
    const csrfToken = document.querySelector('meta[name="_csrf"]').content;

    const socket = new SockJS('/ws');
    const stompClient = Stomp.over(socket);

    // Gửi kèm CSRF Token trong header lúc CONNECT
    stompClient.connect({ [csrfHeader]: csrfToken }, function () {
        stompClient.subscribe('/user/queue/messages', function (msg) {
            console.log(msg.body);
        });
    });
</script>
```

---

## 6. Ví dụ 2: Bảo mật bằng JWT (Stateless)

Phù hợp cho kiến trúc tách biệt Frontend/Backend (React, Next.js, Angular, Mobile App).
> **Lưu ý bảo mật:** Không truyền JWT qua URL kiểu `/ws?token=eyJ...` vì URL sẽ bị lưu lại trong lịch sử trình duyệt, log của Nginx/Load Balancer và log hệ thống. Thay vào đó, hãy truyền token qua **STOMP `CONNECT` Headers**.

### Bước 1: Mở endpoint `/ws/**` ở tầng HTTP (`SecurityConfig.java`)

```java
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf(AbstractHttpConfigurer::disable)
            .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/api/auth/**").permitAll()
                .requestMatchers("/ws/**").permitAll() // Cho phép bắt tay, xác thực JWT ở tầng STOMP
                .anyRequest().authenticated()
            );
        return http.build();
    }
}
```

### Bước 2: Giải mã JWT & Gán `Principal` trong `WebSocketConfig.java`

```java
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Autowired
    private JwtService jwtService; // Service tự viết để giải mã & kiểm tra hạn JWT

    @Autowired
    private UserDetailsService userDetailsService;

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(new ChannelInterceptor() {
            @Override
            public Message<?> preSend(Message<?> message, MessageChannel channel) {
                // Bắt buộc dùng MessageHeaderAccessor.getAccessor để giữ lại thay đổi setUser()
                StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);

                if (accessor != null && StompCommand.CONNECT.equals(accessor.getCommand())) {
                    String authHeader = accessor.getFirstNativeHeader("Authorization");

                    if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                        throw new AccessDeniedException("Thiếu JWT Token trong STOMP header");
                    }

                    String token = authHeader.substring(7);
                    String username = jwtService.extractUsername(token);

                    if (username != null && jwtService.isTokenValid(token)) {
                        UserDetails userDetails = userDetailsService.loadUserByUsername(username);
                        UsernamePasswordAuthenticationToken authentication =
                                new UsernamePasswordAuthenticationToken(userDetails, null, userDetails.getAuthorities());

                        // Gắn danh tính đã xác thực vào phiên WebSocket này
                        accessor.setUser(authentication);
                    } else {
                        throw new AccessDeniedException("JWT Token không hợp lệ hoặc đã hết hạn");
                    }
                }
                return message;
            }
        });
    }
    // ... các hàm configureMessageBroker và registerStompEndpoints giữ nguyên
}
```

### Bước 3: Gửi JWT Header từ Frontend (React / Next.js / JS)

```javascript
const accessToken = localStorage.getItem('access_token');
const socket = new SockJS('http://localhost:8080/ws');
const stompClient = Stomp.over(socket);

stompClient.connect(
    { Authorization: 'Bearer ' + accessToken }, // Gửi vào STOMP CONNECT frame
    function (frame) {
        console.log('Xác thực JWT thành công:', frame.headers['user-name']);
        stompClient.subscribe('/user/queue/messages', (msg) => {
            console.log('Nhận tin:', msg.body);
        });
    },
    function (error) {
        console.error('Từ chối kết nối (Token sai/hết hạn):', error);
    }
);
```

---

## 7. Theo dõi trạng thái Online / Offline (Session Events)

Spring tự động phát ra sự kiện mỗi khi một `Principal` kết nối hoặc ngắt kết nối (đóng tab, rớt mạng):

```java
@Component
public class WebSocketEventListener {

    @EventListener
    public void handleSessionConnected(SessionConnectedEvent event) {
        Principal user = event.getUser();
        if (user != null) {
            System.out.println("[ONLINE] " + user.getName());
            // Cập nhật trạng thái vào Redis / Database
        }
    }

    @EventListener
    public void handleSessionDisconnect(SessionDisconnectEvent event) {
        Principal user = event.getUser();
        if (user != null) {
            System.out.println("[OFFLINE] " + user.getName());
            // Cập nhật trạng thái Offline
        }
    }
}
```

---

## 8. Bảng tóm tắt các lỗi bảo mật & vận hành cần tránh

| Vấn đề thường gặp | Hậu quả | Cách khắc phục chuẩn |
| :--- | :--- | :--- |
| Dùng `setAllowedOrigins("*")` kèm `.withSockJS()` | Lỗi khởi động Spring Boot (CORS cấm `*` khi bật `allowCredentials`) | Dùng `setAllowedOriginPatterns("https://domain-cua-ban.com")` |
| Truyền JWT lên URL `/ws?token=...` | Lộ token qua Access Log của Server, Proxy, Browser History | Truyền JWT qua header của lệnh STOMP `CONNECT` |
| Tin tưởng trường `senderId` từ JSON Client gửi lên | User A có thể giả mạo gửi tin nhắn dưới danh nghĩa User B | Luôn lấy danh tính người gửi từ đối tượng `Principal` trên Server |
| Không kiểm tra lệnh `SUBSCRIBE` trong Interceptor | User A có thể tự subscribe `/user/userB/queue/messages` để đọc trộm tin | Kiểm tra `destination` khớp với `accessor.getUser().getName()` |
| Dùng `StompHeaderAccessor.wrap(message)` khi `setUser()` | Spring không lưu lại `Principal` cho các gói tin tiếp theo | Dùng `MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class)` |