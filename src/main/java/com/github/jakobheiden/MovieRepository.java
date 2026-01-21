package com.github.jakobheiden;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MovieRepository {

    private static Connection dbConnection;

    public MovieRepository() throws SQLException {
        dbConnection = DriverManager.getConnection("jdbc:sqlite:data/movies.db");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                dbConnection.close();
            } catch (SQLException _) {
            }
        }));
    }

    public void initSchema() throws SQLException {
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        imdb_id TEXT PRIMARY KEY,
                        title TEXT,
                        year TEXT,
                        has_been_watched BOOLEAN DEFAULT 0
                    )
                    """);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        message_id TEXT PRIMARY KEY,
                        imdb_id TEXT,
                        FOREIGN KEY (imdb_id) REFERENCES movies(imdb_id)
                    )
                    """);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS likes (
                        imdb_id TEXT,
                        user_id TEXT,
                        PRIMARY KEY (imdb_id, user_id),
                        FOREIGN KEY (imdb_id) REFERENCES movies(imdb_id)
                    )
                    """);
        }
    }

    public boolean persistMovie(String imdbId, String title, String year) throws SQLException {
        String movieSql = "INSERT OR IGNORE INTO movies (imdb_id, title, year) VALUES (?, ?, ?)";
        try (PreparedStatement stmt = dbConnection.prepareStatement(movieSql)) {
            stmt.setString(1, imdbId);
            stmt.setString(2, title);
            stmt.setString(3, year);
            int rowsAffected = stmt.executeUpdate();
            return rowsAffected == 0;
        }
    }

    public void persistMessage(String messageId, String imdbId) throws SQLException {
        String messageSql = "INSERT INTO messages (message_id, imdb_id) VALUES (?, ?)";
        try (PreparedStatement stmt = dbConnection.prepareStatement(messageSql)) {
            stmt.setString(1, messageId);
            stmt.setString(2, imdbId);
            stmt.executeUpdate();
        }
    }

    public record MovieSuggestions(int maxTaggedLikeCount, List<Integer> allLikeCounts, List<String> imdbIds, List<String> titles,
                                   List<String> years) {}

    public MovieSuggestions fetchMovieSuggestions(List<String> mentionedUserIds) throws SQLException {
        String placeholders = String.join(",", mentionedUserIds.stream().map(id -> "?").toList());
        String sql = """
                SELECT\s
                    m.imdb_id,\s
                    m.title,\s
                    m.year,
                    COUNT(CASE WHEN l.user_id IN (%s) THEN 1 END) as tagged_like_count,
                    COUNT(l.user_id) as all_like_count
                FROM movies m
                JOIN likes l ON m.imdb_id = l.imdb_id
                WHERE m.has_been_watched = 0
                GROUP BY m.imdb_id
                HAVING tagged_like_count > 0
                ORDER BY tagged_like_count DESC, all_like_count ASC
            """.formatted(placeholders);

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            for (int i = 0; i < mentionedUserIds.size(); i++) {
                stmt.setString(i + 1, mentionedUserIds.get(i));
            }

            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                return new MovieSuggestions(0, List.of(), List.of(), List.of(), List.of());
            }

            int maxTaggedLikeCount = rs.getInt("tagged_like_count");
            List<String> titles = new ArrayList<>();
            List<String> years = new ArrayList<>();
            List<String> imdbIds = new ArrayList<>();
            List<Integer> allLikeCounts = new ArrayList<>();
            do {
                if (rs.getInt("tagged_like_count") < maxTaggedLikeCount) break;
                imdbIds.add(rs.getString("imdb_id"));
                titles.add(rs.getString("title"));
                years.add(rs.getString("year"));
                allLikeCounts.add(rs.getInt("all_like_count"));
            } while (rs.next());

            return new MovieSuggestions(maxTaggedLikeCount, allLikeCounts, imdbIds, titles, years);
        }
    }

    public Optional<String> fetchImdbIdFromMessageId(String messageId) throws SQLException {
        String selectSql = "SELECT imdb_id FROM messages WHERE message_id = ?";
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(selectSql)) {
            preparedStatement.setString(1, messageId);
            var resultSet = preparedStatement.executeQuery();
            if (!resultSet.next()) {
                return Optional.empty();
            }
            return Optional.of(resultSet.getString("imdb_id"));
        }
    }

    public void persistLike(String userId, String imdbId) throws SQLException {
        String insertSql = "INSERT INTO likes (imdb_id, user_id) VALUES (?, ?)";
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(insertSql)) {
            preparedStatement.setString(1, imdbId);
            preparedStatement.setString(2, userId);
            preparedStatement.executeUpdate();
            IO.println("Like added: user " + userId + " liked movie " + imdbId);
        } catch (SQLException e) {
            if (e.getMessage().contains("UNIQUE constraint failed") ||
                    e.getMessage().contains("PRIMARY KEY")) {
                IO.println("User " + userId + " already liked movie (duplicate ignored)");
            } else {
                throw e;
            }
        }
    }

    public void deleteLike(String imdbId, String userId) throws SQLException {
        String deleteSql = "DELETE FROM likes WHERE imdb_id = ? AND user_id = ?";
        int rowsAffected;
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(deleteSql)) {
            preparedStatement.setString(1, imdbId);
            preparedStatement.setString(2, userId);
            rowsAffected = preparedStatement.executeUpdate();
        }

        if (rowsAffected == 0) {
            IO.println("No like to remove: user " + userId + " had not liked movie " + imdbId);
        } else {
            IO.println("Like removed: user " + userId + " unliked movie " + imdbId);
        }
    }

    public boolean markMovieAsSeen(String imdbId) throws SQLException {
        String updateSql = "UPDATE movies SET has_been_watched = 1 WHERE imdb_id = ? AND has_been_watched = 0";
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(updateSql)) {
            preparedStatement.setString(1, imdbId);
            return preparedStatement.executeUpdate() == 0;
        }
    }

    public boolean markMovieAsNotSeen(String imdbId) throws SQLException {
        String updateSql = "UPDATE movies SET has_been_watched = 0 WHERE imdb_id = ? AND has_been_watched = 1";
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(updateSql)) {
            preparedStatement.setString(1, imdbId);
            return preparedStatement.executeUpdate() == 0;
        }
    }

    public List<String> fetchMessageIds(String imdbId) throws SQLException {
        String findMessagesForMovieSql = "SELECT message_id FROM messages WHERE imdb_id = ?";
        List<String> messageIds = new ArrayList<>();
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(findMessagesForMovieSql)) {
            preparedStatement.setString(1, imdbId);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                messageIds.add(resultSet.getString("message_id"));
            }
        }
        return messageIds;
    }
}
