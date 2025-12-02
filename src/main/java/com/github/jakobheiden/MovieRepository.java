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

    public boolean persistMovie(String imdbId, String title) throws SQLException {
        String movieSql = "INSERT OR IGNORE INTO movies (imdb_id, title) VALUES (?, ?)";
        try (PreparedStatement stmt = dbConnection.prepareStatement(movieSql)) {
            stmt.setString(1, imdbId);
            stmt.setString(2, title);
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

    public record MovieSuggestions(int likeCount, List<String> movieTitles) {}

    public MovieSuggestions fetchMovieSuggestions(List<String> mentionedUserIds) throws SQLException {
        String placeholders = String.join(",", mentionedUserIds.stream().map(id -> "?").toList());
        String sql = """
            SELECT m.imdb_id, m.title, COUNT(l.user_id) as like_count
            FROM movies m
            JOIN likes l ON m.imdb_id = l.imdb_id
            WHERE l.user_id IN (%s) AND m.has_been_watched = 0
            GROUP BY m.imdb_id
            ORDER BY like_count DESC
            """.formatted(placeholders);

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            for (int i = 0; i < mentionedUserIds.size(); i++) {
                stmt.setString(i + 1, mentionedUserIds.get(i));
            }

            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                return new MovieSuggestions(0, List.of());
            }

            int maxLikes = rs.getInt("like_count");
            List<String> titles = new ArrayList<>();
            titles.add(rs.getString("title"));

            while (rs.next()) {
                int likes = rs.getInt("like_count");
                if (likes < maxLikes) break;
                titles.add(rs.getString("title"));
            }

            return new MovieSuggestions(maxLikes, titles);
        }
    }

    public String fetchImdbIdFromTitle(String movieTitle) throws SQLException {
        IO.println(movieTitle);
        String findMovieIdSql = "SELECT imdb_id FROM movies WHERE title = ?";
        try (PreparedStatement preparedStatement = dbConnection.prepareStatement(findMovieIdSql)) {
            preparedStatement.setString(1, movieTitle);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (!resultSet.next()) throw new SQLException("Movie not found in database: " + movieTitle);

            return resultSet.getString("imdb_id");
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

    public List<String> fetchMovieIds(String imdbId) throws SQLException {
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
