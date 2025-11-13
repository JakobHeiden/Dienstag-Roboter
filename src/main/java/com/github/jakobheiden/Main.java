package com.github.jakobheiden;

import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.MessageCreateEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

    private final static long filmeChannelId = 1083096195825680505L;
    private static Connection dbConnection;

    static {
        try {
            dbConnection = DriverManager.getConnection("jdbc:sqlite:filme.db");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                imdb_id TEXT,
                user_id TEXT,
                FOREIGN KEY (imdb_id) REFERENCES movies(imdb_id),
                UNIQUE(imdb_id, user_id)
            )
            """);
        }
    }

    static void main() throws SQLException {
        String token = System.getenv("DISCORD_BOT_TOKEN");
        if (token == null) {
            System.err.println("DISCORD_BOT_TOKEN environment variable not set");
            System.exit(1);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { dbConnection.close(); } catch (SQLException e) { }
        }));

        initSchema();

        GatewayDiscordClient client = DiscordClientBuilder.create(token)
                .build()
                .login()
                .block();

        client.getEventDispatcher().on(ReadyEvent.class)
                .subscribe(event -> {
                    System.out.println("Bot logged in as " + event.getSelf().getUsername());
                });

        client.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(Main::isFilmeChannel)
                .filter(Main::isImdbLink)
                .subscribe(Main::persistMovie);

        client.onDisconnect().block();
    }

    private static boolean isImdbLink(MessageCreateEvent event) {
        return event.getMessage().getContent().toLowerCase().contains("imdb.com/title/tt");
    }

    private static boolean isFilmeChannel(MessageCreateEvent event) {
        return event.getMessage().getChannelId().asLong() == filmeChannelId;
    }

    private static void persistMovie(MessageCreateEvent event) {
        // TODO
    }
}
