package com.github.jakobheiden;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;
import discord4j.core.event.domain.message.ReactionAddEvent;
import discord4j.core.object.emoji.Emoji;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.object.emoji.UnicodeEmoji;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Path;
import java.nio.file.Files;

public class Main {

    private static final long filmeChannelId = 1083096195825680505L;
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
        dbConnection = DriverManager.getConnection("jdbc:sqlite:data/filme.db");
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
                .filter(Main::isFilmeChannel)
                .filter(Main::isImdbLink)
                .subscribe(Main::persistMovie, Main::handleException);

        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(Main::isReactionInFilmeChannel)
                .filter(Main::isThumbsUp)
                .subscribe(Main::handleLikeReaction, Main::handleException);

        discordClient.onDisconnect().block();
    }

    private static boolean isImdbLink(MessageCreateEvent event) {
        return event.getMessage().getContent().toLowerCase().contains("imdb.com/title/tt");
    }

    private static boolean isFilmeChannel(MessageCreateEvent event) {
        return event.getMessage().getChannelId().asLong() == filmeChannelId || event.getMessage().getChannelId().asLong() == testChannelId;
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

        discordClient.getChannelById(discord4j.common.util.Snowflake.of(filmeChannelId))
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

    private static boolean isReactionInFilmeChannel(ReactionAddEvent event) {
        long channelId = event.getChannelId().asLong();
        return channelId == filmeChannelId || channelId == testChannelId;
    }

    private static boolean isThumbsUp(ReactionAddEvent event) {
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
            try (PreparedStatement stmt = dbConnection.prepareStatement(selectSql)) {
                stmt.setString(1, messageId);
                var rs = stmt.executeQuery();
                if (!rs.next()) {
                    // Not a movie message, ignore silently
                    return;
                }
                imdbId = rs.getString("imdb_id");
            }

            String insertSql = "INSERT INTO likes (imdb_id, user_id) VALUES (?, ?)";
            try (PreparedStatement stmt = dbConnection.prepareStatement(insertSql)) {
                stmt.setString(1, imdbId);
                stmt.setString(2, userId);
                stmt.executeUpdate();
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
}