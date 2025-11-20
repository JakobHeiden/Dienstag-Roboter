package com.github.jakobheiden;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MovieRepository {
    private final Connection connection;

    public MovieRepository(Connection connection) {
        this.connection = connection;
    }

    public void initSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        imdb_id TEXT PRIMARY KEY,
                        title TEXT,
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

    public boolean insertMovie(String imdbId, String title) throws SQLException {
        String movieSql = "INSERT OR IGNORE INTO movies (imdb_id, title) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(movieSql)) {
            stmt.setString(1, imdbId);
            stmt.setString(2, title);
            int rowsAffected = stmt.executeUpdate();
            return rowsAffected > 0;
        }
    }

    public Optional<String> getImdbIdByTitle(String title) throws SQLException {
        String sql = "SELECT imdb_id FROM movies WHERE title = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, title);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Optional.of(rs.getString("imdb_id"));
            }
            return Optional.empty();
        }
    }

    public void markMovieAsWatched(String imdbId) throws SQLException {
        String sql = "UPDATE movies SET has_been_watched = 1 WHERE imdb_id = ? AND has_been_watched = 0";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, imdbId);
            stmt.executeUpdate();
        }
    }

    public boolean wasMovieMarked(String imdbId) throws SQLException {
        String sql = "UPDATE movies SET has_been_watched = 1 WHERE imdb_id = ? AND has_been_watched = 0";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, imdbId);
            int affectedRows = stmt.executeUpdate();
            return affectedRows > 0;
        }
    }

    public void insertMessage(String messageId, String imdbId) throws SQLException {
        String sql = "INSERT INTO messages (message_id, imdb_id) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, messageId);
            stmt.setString(2, imdbId);
            stmt.executeUpdate();
        }
    }

    public Optional<String> getImdbIdForMessage(String messageId) throws SQLException {
        String sql = "SELECT imdb_id FROM messages WHERE message_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, messageId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Optional.of(rs.getString("imdb_id"));
            }
            return Optional.empty();
        }
    }

    public List<String> getMessageIdsForMovie(String imdbId) throws SQLException {
        String sql = "SELECT message_id FROM messages WHERE imdb_id = ?";
        List<String> messageIds = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, imdbId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                messageIds.add(rs.getString("message_id"));
            }
        }
        return messageIds;
    }

    public boolean addLike(String imdbId, String userId) throws SQLException {
        String sql = "INSERT INTO likes (imdb_id, user_id) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, imdbId);
            stmt.setString(2, userId);
            stmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (e.getMessage().contains("UNIQUE constraint failed") ||
                e.getMessage().contains("PRIMARY KEY")) {
                return false;
            }
            throw e;
        }
    }

    public boolean removeLike(String imdbId, String userId) throws SQLException {
        String sql = "DELETE FROM likes WHERE imdb_id = ? AND user_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, imdbId);
            stmt.setString(2, userId);
            int rowsAffected = stmt.executeUpdate();
            return rowsAffected > 0;
        }
    }

    public List<MovieSuggestion> getSuggestedMovies(List<String> userIds) throws SQLException {
        String placeholders = String.join(",", userIds.stream().map(id -> "?").toList());
        String sql = """
                SELECT m.imdb_id, m.title, COUNT(l.user_id) as like_count
                FROM movies m
                JOIN likes l ON m.imdb_id = l.imdb_id
                WHERE l.user_id IN (%s) AND m.has_been_watched = 0
                GROUP BY m.imdb_id
                ORDER BY like_count DESC
                """.formatted(placeholders);

        List<MovieSuggestion> suggestions = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < userIds.size(); i++) {
                stmt.setString(i + 1, userIds.get(i));
            }
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                suggestions.add(new MovieSuggestion(
                    rs.getString("imdb_id"),
                    rs.getString("title"),
                    rs.getInt("like_count")
                ));
            }
        }
        return suggestions;
    }

    public record MovieSuggestion(String imdbId, String title, int likeCount) {}
}
