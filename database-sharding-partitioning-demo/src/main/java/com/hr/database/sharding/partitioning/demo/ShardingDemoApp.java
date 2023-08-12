package com.hr.database.sharding.partitioning.demo;

import com.hr.database.sharding.partitioning.demo.sharding.SQLTodoRepository;
import com.hr.database.sharding.partitioning.demo.sharding.Todo;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ShardingDemoApp {

    private static final Random RANDOM = new Random();
    private static final char[] CHARS = "abcdfghijklmnopqrstwvuxyz0123456789".toCharArray();

    public static void main(String[] args) throws Exception {
        int shards = 3;
        int todos = 100;

        List<JdbcTemplate> shardsJdbcTemplates = shardsJdbcTemplates(shards);

        List<Todo> todosToInsert = Stream.generate(ShardingDemoApp::randomTodo)
                .limit(todos)
                .toList();

        SQLTodoRepository repository = SQLTodoRepository.ofRangeShards(shardsJdbcTemplates);

        ExecutorService executor = Executors.newFixedThreadPool(50);
        todosToInsert.forEach(t -> executor.submit(() -> repository.create(t)));
        executor.shutdown();

        if (executor.awaitTermination(30, TimeUnit.SECONDS)) {
            System.out.println("Data created!");
        } else {
            System.out.println("Didn't finish creating data in 30 seconds...");
        }
        System.out.println(repository.ofId(230279).get());
        System.out.println(repository.ofId(834250).get());

        System.out.println(repository.allOfNameLike("az").size());
    }

    private static List<JdbcTemplate> shardsJdbcTemplates(int shards) {
        return IntStream.range(0, shards)
                .mapToObj(i -> {
                    try {
                        Connection connection = DriverManager.getConnection(
                                "jdbc:postgresql://localhost:555%d/postgres".formatted(i),
                                "postgres", "postgres");

                        return new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }

    private static Todo randomTodo() {
        long id = 1 + RANDOM.nextLong(SQLTodoRepository.MAX_ID);
        String name = randomString();
        String description = randomString();

        return new Todo(id, name, description);
    }

    private static String randomString() {
        char[] chars = new char[1 + RANDOM.nextInt(50)];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = CHARS[RANDOM.nextInt(CHARS.length)];
        }
        return new String(chars);
    }
}