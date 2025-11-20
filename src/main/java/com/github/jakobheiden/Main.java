package com.github.jakobheiden;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import discord4j.common.util.Snowflake;
import discord4j.core.DiscordClientBuilder;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.event.domain.lifecycle.ReadyEvent;
import discord4j.core.event.domain.message.*;
import discord4j.core.object.emoji.Emoji;
import discord4j.core.object.entity.Message;
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
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.file.Path;
import java.nio.file.Files;

public class Main {

    private static final long movieChannelId = 1083096195825680505L;
    private static final long testChannelId = 1437574563528704101L;
    private static final String ownerMention = "<@622111772979101706>";
    private static final UnicodeEmoji eyesEmoji = UnicodeEmoji.of("\uD83D\uDC40");
    private static String omdbApiKey;
    private static final String OMDB_API_URL_TEMPLATE = "https://www.omdbapi.com/?apikey=%s&i=%s";
    private static GatewayDiscordClient discordClient;
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final Pattern IMDB_ID_PATTERN = Pattern.compile("imdb\\.com/title/(tt\\d+)", Pattern.CASE_INSENSITIVE);
    private static MovieRepository movieRepository;

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

        Path dataDir = Path.of("data");
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        Connection dbConnection = DriverManager.getConnection("jdbc:sqlite:data/movies.db");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                dbConnection.close();
            } catch (SQLException _) {
            }
        }));

        movieRepository = new MovieRepository(dbConnection);
        movieRepository.initSchema();

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

        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(Main::isReactionInMovieChannel)
                .filter(Main::isEyesEmoji)
                .subscribe(Main::handleMarkMovieAsSeenReaction, Main::handleException);

        discordClient.onDisconnect().block();
    }

    private static boolean isImdbLink(MessageCreateEvent event) {
        return event.getMessage().getContent().toLowerCase().contains("imdb.com/title/tt");
    }

    private static boolean hasBotMention(MessageCreateEvent event) {
        return event.getMessage().getUserMentions().stream()
                .map(User::getId)
                .anyMatch(id -> id.equals(discordClient.getSelfId()));
    }

    private static boolean isInFilmeChannel(MessageCreateEvent event) {
        return event.getMessage().getChannelId().asLong() == movieChannelId || event.getMessage().getChannelId().asLong() == testChannelId;
    }

    private static boolean isEyesEmoji(ReactionAddEvent event) {
        return event.getEmoji().equals(eyesEmoji);
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

    private static boolean isReactionInMovieChannel(ReactionEvent event) {
        long channelId = event.getChannelId().asLong();
        return channelId == movieChannelId || channelId == testChannelId;
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

    private static void persistMovie(MessageCreateEvent event) {
        try {
            String messageContent = event.getMessage().getContent();
            String messageId = event.getMessage().getId().asString();
            String imdbId = extractImdbId(messageContent);
            if (imdbId == null) {
                throw new IOException("Failed to extract IMDB ID from message: " + messageId);
            }

            String title = fetchMovieTitle(imdbId);

            boolean inserted = movieRepository.insertMovie(imdbId, title);
            movieRepository.insertMessage(messageId, imdbId);

            String authorId = event.getMessage().getAuthor().get().getId().asString();
            persistLike(messageId, authorId);

            if (inserted) {
                IO.println("Successfully persisted movie: " + title + " (" + imdbId + ")");
            } else {
                IO.println("Movie already in database: " + title + " (" + imdbId + ")");
            }

            replaceImdbLinkWithShareImdbLink(event.getMessage());
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void replaceImdbLinkWithShareImdbLink(Message message) {
        String messageContent = message.getContent();
        String newContent = messageContent.replaceAll("(?i)imdb\\.com", "shareimdb.com");
        if (!newContent.equals(messageContent)) {
            message.edit().withContentOrNull(newContent).subscribe();
        }
    }

    private static void suggestMovie(MessageCreateEvent event) {
        try {
            List<String> mentionedUserIds = event.getMessage().getUserMentions()
                    .stream().filter(user -> !user.isBot())
                    .map(user -> user.getId().asString()).toList();

            if (mentionedUserIds.isEmpty()) return;

            List<MovieRepository.MovieSuggestion> suggestions = movieRepository.getSuggestedMovies(mentionedUserIds);

            if (suggestions.isEmpty()) {
                IO.println("No movies to suggest");
                event.getMessage().getChannel().subscribe(messageChannel ->
                        messageChannel.createMessage("No movies to suggest").subscribe());
                return;
            }

            List<String> movieTitles = new ArrayList<>();
            int maxLikes = suggestions.get(0).likeCount();
            for (MovieRepository.MovieSuggestion suggestion : suggestions) {
                if (suggestion.likeCount() < maxLikes) break;
                movieTitles.add(suggestion.title());
            }
            Collections.shuffle(movieTitles);

            MessageChannel channel = event.getMessage().getChannel().block();
            movieTitles.forEach(movieTitle -> channel.createMessage(movieTitle)
                    .map(Message::getId)
                    .map(Snowflake::asString)
                    .subscribe(messageID -> persistMessageForMovie(messageID, movieTitle), Main::handleException));
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void persistMessageForMovie(String messageId, String movieTitle) {
        try {
            Optional<String> imdbId = movieRepository.getImdbIdByTitle(movieTitle);
            if (imdbId.isEmpty()) {
                throw new SQLException("Movie not found in database: " + movieTitle);
            }

            movieRepository.insertMessage(messageId, imdbId.get());
            IO.println("Persisted movie message: " + messageId + " (" + movieTitle + ")");
        } catch (Exception e) {
            handleException(e);
        }
    }

    private static void handleLikeReaction(ReactionAddEvent event) {
        String messageId = event.getMessageId().asString();
        String userId = event.getUserId().asString();
        persistLike(messageId, userId);
    }

    private static void persistLike(String messageId, String userId) {
        try {
            Optional<String> imdbId = movieRepository.getImdbIdForMessage(messageId);
            if (imdbId.isEmpty()) {
                return;
            }

            boolean added = movieRepository.addLike(imdbId.get(), userId);
            if (added) {
                IO.println("Like added: user " + userId + " liked movie " + imdbId.get());
            } else {
                IO.println("User " + userId + " already liked movie (duplicate ignored)");
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void handleUnlikeReaction(ReactionRemoveEvent event) {
        String messageId = event.getMessageId().asString();
        String userId = event.getUserId().asString();

        try {
            Optional<String> imdbId = movieRepository.getImdbIdForMessage(messageId);
            if (imdbId.isEmpty()) {
                return;
            }

            boolean removed = movieRepository.removeLike(imdbId.get(), userId);
            if (removed) {
                IO.println("Like removed: user " + userId + " unliked movie " + imdbId.get());
            } else {
                IO.println("No like to remove: user " + userId + " had not liked movie " + imdbId.get());
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void handleMarkMovieAsSeenReaction(ReactionAddEvent event) {
        String messageId = event.getMessageId().asString();
        try {
            Optional<String> imdbId = movieRepository.getImdbIdForMessage(messageId);
            if (imdbId.isEmpty()) {
                IO.println("Not a movie message: " + messageId);
                return;
            }

            boolean marked = movieRepository.wasMovieMarked(imdbId.get());
            if (!marked) {
                IO.println("Movie already marked as seen: " + imdbId.get());
                return;
            }

            IO.println("Movie marked as seen: " + imdbId.get());

            List<String> messageIds = movieRepository.getMessageIdsForMovie(imdbId.get());
            for (String messageToMarkId : messageIds) {
                discordClient.getChannelById(Snowflake.of(movieChannelId))
                        .ofType(MessageChannel.class)
                        .flatMap(channel -> channel.getMessageById(discord4j.common.util.Snowflake.of(messageToMarkId)))
                        .subscribe(message -> message.addReaction(eyesEmoji).subscribe());
            }
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void handleException(Throwable throwable) {
        String errorMessage = throwable.getMessage();
        System.err.println(errorMessage);

        discordClient.getChannelById(discord4j.common.util.Snowflake.of(movieChannelId))
                .ofType(MessageChannel.class)
                .flatMap(channel -> channel.createMessage("⚠️ Error: " + errorMessage + " " + ownerMention))
                .subscribe();
    }
}