package com.hr.database.sharding.partitioning.demo.sharding;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SQLTodoRepository {
    public static final long MAX_ID = 1_000_000;
    private static final TodoMapper TODO_MAPPER = new TodoMapper();

    private List<JdbcTemplate> shardTemplates;
    private ShardResolver shardResolver;

    public SQLTodoRepository(List<JdbcTemplate> shardTemplates, ShardResolver shardResolver) {
        this.shardTemplates = shardTemplates;
        this.shardResolver = shardResolver;
    }

    public static SQLTodoRepository ofHashShards(List<JdbcTemplate> shardTemplates)
    {
        int shardsCount = shardTemplates.size();
        return new SQLTodoRepository(shardTemplates, id-> Objects.hash(id) % shardsCount);
    }

    public static SQLTodoRepository ofRangeShards(List<JdbcTemplate> shardsTemplates)
    {
        int shards = shardsTemplates.size();
        List<Long> shardsMaxValues = new ArrayList<Long>();

        long nextMax = (long) (MAX_ID / shards);
        long step = nextMax;
        for (int i = 0; i < shardsTemplates.size(); i++) {
            shardsMaxValues.add(nextMax);
            nextMax += step;
        }
        System.out.println("Shards..." + shardsMaxValues);


        return new SQLTodoRepository(shardsTemplates,
                id -> {
                    int shard = 0;
                    for (Long maxShard : shardsMaxValues) {
                        if (maxShard > id) {
                            return shard;
                        }
                        shard++;
                    }

                    return shardsMaxValues.size() - 1;
                });
    }

    public List<Todo> allOfNameLike(String name) {
        System.out.println("-------- Retrieving Result from all shards-------");
        return shardTemplates.parallelStream()
                .map(sht -> sht.query("SELECT * FROM todo WHERE name like ?",
                        TODO_MAPPER, "%" + name + "%"))
                .flatMap(Collection::stream)
                .toList();
    }

    public void create(Todo todo) {
        JdbcTemplate shard = shardJdbcTemplate(todo.getId());

        shard.update("INSERT INTO todo (id, name, description) values (?, ?, ?)",
                todo.getId(), todo.getName(), todo.getDescription());
    }

    public Optional<Todo> ofId(long id) {
        System.out.println("-------- Retrieving Result from single shard-------");
        List<Todo> result = shardJdbcTemplate(id)
                .query("SELECT * FROM todo WHERE id = ?", TODO_MAPPER, id);

        return result.isEmpty() ? Optional.empty() : Optional.ofNullable(result.get(0));
    }

    private JdbcTemplate shardJdbcTemplate(long id) {
        int shardIdx = shardResolver.shardForId(id);
        System.out.println("For id %d, Using %d shard...".formatted(id, shardIdx));
        return shardTemplates.get(shardIdx);
    }












    private static class TodoMapper implements RowMapper<Todo> {

        @Override
        public Todo mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new Todo(rs.getLong("id"), rs.getString("name"),
                    rs.getString("description"));
        }
    }

    public interface ShardResolver {
        int shardForId(long id);
    }
}


