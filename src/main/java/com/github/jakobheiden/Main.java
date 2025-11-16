package com.github.jakobheiden;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.*;
import discord4j.core.object.emoji.Emoji;
import discord4j.core.object.entity.User;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.object.emoji.UnicodeEmoji;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Path;
import java.nio.file.Files;

public class Main {

    private static final long movieChannelId = 1083096195825680505L;
    private static final long testChannelId = 1437574563528704101L;
    private static final String ownerMention = "<@622111772979101706>";
    private static String omdbApiKey;
    private static final String OMDB_API_URL_TEMPLATE = "https://www.omdbapi.com/?apikey=%s&i=%s";
    private static GatewayDiscordClient discordClient;
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Pattern IMDB_ID_PATTERN = Pattern.compile("imdb\\.com/title/(tt\\d+)", Pattern.CASE_INSENSITIVE);
    private static Connection dbConnection;

    private static void initSchema() throws SQLException {
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

    static void main() throws Exception {
        String token = System.getenv("DISCORD_BOT_TOKEN");
        if (token == null) {
            System.err.println("DISCORD_BOT_TOKEN environment variable not set");
            System.exit(1);
        }

        omdbApiKey = System.getenv("OMDB_API_KEY");
        if (omdbApiKey == null) {
            System.err.println("OMDB_API_KEY environment variable not set");
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                dbConnection.close();
            } catch (SQLException _) {
            }
        }));
        Path dataDir = Path.of("data");
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        dbConnection = DriverManager.getConnection("jdbc:sqlite:data/movies.db");
        initSchema();

        Hooks.onErrorDropped(Main::handleException);

        discordClient = DiscordClientBuilder.create(token)
                .build()
                .login()
                .block();

        discordClient.getEventDispatcher().on(ReadyEvent.class)
                .subscribe(event -> {
                    IO.println("Bot logged in as " + event.getSelf().getUsername());
                });

        discordClient.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(Main::isInFilmeChannel)
                .filter(Main::isImdbLink)
                .subscribe(Main::persistMovie, Main::handleException);

        discordClient.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(Main::isInFilmeChannel)
                .filter(Main::hasBotMention)
                .subscribe(Main::suggestMovie, Main::handleException);

        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(Main::isReactionInMovieChannel)
                .filter(Main::isThumbsUp)
                .subscribe(Main::handleLikeReaction, Main::handleException);

        discordClient.getEventDispatcher().on(ReactionRemoveEvent.class)
                .filter(Main::isReactionInMovieChannel)
                .filter(Main::isThumbsUp)
                .subscribe(Main::handleUnlikeReaction, Main::handleException);

        discordClient.onDisconnect().block();
    }

    private static boolean isImdbLink(MessageCreateEvent event) {
        return event.getMessage().getContent().toLowerCase().contains("imdb.com/title/tt");
    }

    private static boolean hasBotMention(MessageCreateEvent event) {
        return event.getMessage().getUserMentions().stream()
                .map(user -> user.getId())
                .anyMatch(id -> id.equals(discordClient.getSelfId()));
    }

    private static boolean isInFilmeChannel(MessageCreateEvent event) {
        return event.getMessage().getChannelId().asLong() == movieChannelId || event.getMessage().getChannelId().asLong() == testChannelId;
    }

    private static String fetchMovieTitle(String imdbId) throws IOException, InterruptedException {
        String url = String.format(OMDB_API_URL_TEMPLATE, omdbApiKey, imdbId);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("HTTP error: " + response.statusCode());
        }

        JsonObject json = JsonParser.parseString(response.body()).getAsJsonObject();
        if (!json.has("Response") || !json.get("Response").getAsString().equals("True")) {
            String jsonError = json.has("Error") ? json.get("Error").getAsString() : "Could not parse json from response";
            String error = "OMDb API error: " + jsonError;
            throw new RuntimeException(error);
        }

        return json.get("Title").getAsString();
    }

    private static String extractImdbId(String message) {
        Matcher matcher = IMDB_ID_PATTERN.matcher(message);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private static void handleException(Throwable throwable) {
        String errorMessage = throwable.getMessage();
        System.err.println(errorMessage);

        discordClient.getChannelById(discord4j.common.util.Snowflake.of(movieChannelId))
                .ofType(MessageChannel.class)
                .flatMap(channel -> channel.createMessage("⚠️ Error: " + errorMessage + " " + ownerMention))
                .subscribe();
    }

    private static void persistMovie(MessageCreateEvent event) {
        try {
            String messageContent = event.getMessage().getContent();
            String messageId = event.getMessage().getId().asString();
            String imdbId = extractImdbId(messageContent);
            if (imdbId == null) {
                throw new IOException("Failed to extract IMDB ID from message: " + messageId);
            }

            String title = fetchMovieTitle(imdbId);

            String movieSql = "INSERT OR IGNORE INTO movies (imdb_id, title) VALUES (?, ?)";
            int rowsAffected;
            try (PreparedStatement stmt = dbConnection.prepareStatement(movieSql)) {
                stmt.setString(1, imdbId);
                stmt.setString(2, title);
                rowsAffected = stmt.executeUpdate();
            }

            String messageSql = "INSERT INTO messages (message_id, imdb_id) VALUES (?, ?)";
            try (PreparedStatement stmt = dbConnection.prepareStatement(messageSql)) {
                stmt.setString(1, messageId);
                stmt.setString(2, imdbId);
                stmt.executeUpdate();
            }

            if (rowsAffected == 0) {
                IO.println("Movie already in database: " + title + " (" + imdbId + ")");
            } else {
                IO.println("Successfully persisted movie: " + title + " (" + imdbId + ")");
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void suggestMovie(MessageCreateEvent event) {
        try {
            List<String> mentionedUserIds = event.getMessage().getUserMentions()
                    .stream().filter(user -> !user.isBot())
                    .map(user -> user.getId().asString()).toList();

            if (mentionedUserIds.isEmpty()) return;

            String placeholders = String.join(",", mentionedUserIds.stream().map(id -> "?").toList());
            String sql = """
                    SELECT m.imdb_id, m.title, COUNT(l.user_id) as like_count
                    FROM movies m
                    JOIN likes l ON m.imdb_id = l.imdb_id
                    WHERE l.user_id IN (%s) AND m.has_been_watched = 0
                    GROUP BY m.imdb_id
                    ORDER BY like_count DESC
                    """.formatted(placeholders);

            try (PreparedStatement preparedStatement = dbConnection.prepareStatement(sql)) {
                for (int i = 0; i < mentionedUserIds.size(); i++) {
                    preparedStatement.setString(i + 1, mentionedUserIds.get(i));
                }

                ResultSet resultSet = preparedStatement.executeQuery();

                if (!resultSet.next()) {
                    IO.println("No movies to suggest");
                    event.getMessage().getChannel().subscribe(messageChannel ->
                            messageChannel.createMessage("No movies to suggest").subscribe());
                    return;
                }

                List<String> movieTitles = new ArrayList<>(resultSet.getMetaData().getColumnCount());
                movieTitles.add(resultSet.getString("title"));
                int maxLikes = resultSet.getInt("like_count");
                while (resultSet.next()) {
                    if (resultSet.getInt("like_count") < maxLikes) break;

                    movieTitles.add(resultSet.getString("title"));
                }
                Collections.shuffle(movieTitles);

                MessageChannel channel = event.getMessage().getChannel().block();
                movieTitles.forEach(movieTitle -> channel.createMessage(movieTitle).subscribe());
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static boolean isReactionInMovieChannel(ReactionEvent event) {
        long channelId = event.getChannelId().asLong();
        return channelId == movieChannelId || channelId == testChannelId;
    }

    private static boolean isThumbsUp(ReactionBaseEmojiEvent event) {
        Emoji emoji = event.getEmoji();
        if (emoji instanceof UnicodeEmoji unicodeEmoji) {
            String raw = unicodeEmoji.getRaw();
            // Match 👍 and all skin tone variants
            return raw.startsWith("👍");
        }
        return false;
    }

    private static void handleLikeReaction(ReactionAddEvent event) {
        String messageId = event.getMessageId().asString();
        String userId = event.getUserId().asString();

        try {
            String selectSql = "SELECT imdb_id FROM messages WHERE message_id = ?";
            String imdbId;
            try (PreparedStatement preparedStatement = dbConnection.prepareStatement(selectSql)) {
                preparedStatement.setString(1, messageId);
                var resultSet = preparedStatement.executeQuery();
                if (!resultSet.next()) {
                    // Not a movie message, ignore silently
                    return;
                }
                imdbId = resultSet.getString("imdb_id");
            }

            String insertSql = "INSERT INTO likes (imdb_id, user_id) VALUES (?, ?)";
            try (PreparedStatement preparedStatement = dbConnection.prepareStatement(insertSql)) {
                preparedStatement.setString(1, imdbId);
                preparedStatement.setString(2, userId);
                preparedStatement.executeUpdate();
            }

            IO.println("Like added: user " + userId + " liked movie " + imdbId);
        } catch (SQLException e) {
            if (e.getMessage().contains("UNIQUE constraint failed") ||
                    e.getMessage().contains("PRIMARY KEY")) {
                IO.println("User " + userId + " already liked movie (duplicate ignored)");
            } else {
                handleException(e);
            }
        }
    }

    private static void handleUnlikeReaction(ReactionRemoveEvent event) {
        String messageId = event.getMessageId().asString();
        String userId = event.getUserId().asString();

        try {
            String selectSql = "SELECT imdb_id FROM messages WHERE message_id = ?";
            String imdbId;
            try (PreparedStatement preparedStatement = dbConnection.prepareStatement(selectSql)) {
                preparedStatement.setString(1, messageId);
                ResultSet resultSet = preparedStatement.executeQuery();
                if (!resultSet.next()) {
                    // Not a movie message, ignore silently
                    return;
                }
                imdbId = resultSet.getString("imdb_id");
            }

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
        } catch (SQLException e) {
            handleException(e);
        }
    }
}