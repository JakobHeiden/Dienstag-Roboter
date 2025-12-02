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
import reactor.core.publisher.Flux;
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

public class App {

    private final long movieChannelId;
    private final String ownerMention;
    private final Snowflake botSnowflake;
    private static final UnicodeEmoji eyesEmoji = UnicodeEmoji.of("\uD83D\uDC40");
    private static final String OMDB_API_URL_TEMPLATE = "https://www.omdbapi.com/?apikey=%s&i=%s";
    private static final Pattern IMDB_ID_PATTERN = Pattern.compile("imdb\\.com/title/(tt\\d+)", Pattern.CASE_INSENSITIVE);

    private final String omdbApiKey;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final MovieRepository movieRepository;
    private final GatewayDiscordClient discordClient;

    static void main(String[] args) throws Exception {
        new App();
    }

    public App() throws SQLException, IOException {
        SettingsLoader settingsLoader = new SettingsLoader("settings.yaml");
        String token = settingsLoader.getDiscordBotToken();
        omdbApiKey = settingsLoader.getOmdbApiKey();
        movieChannelId = settingsLoader.getMovieChannelId();
        ownerMention = String.format("<@%d>", settingsLoader.getOwnerId());
        botSnowflake = Snowflake.of(settingsLoader.getBotId());

        Path dataDir = Path.of("data");
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }

        movieRepository = new MovieRepository();
        movieRepository.initSchema();

        discordClient = DiscordClientBuilder.create(token)
                .build()
                .login()
                .block();
        Hooks.onErrorDropped(throwable -> handleException(throwable));
        configureEventHandlers(token);
        discordClient.onDisconnect().block();
    }

    private void configureEventHandlers(String token) {
        discordClient.getEventDispatcher().on(ReadyEvent.class)
                .subscribe(event -> {
                    IO.println("Bot logged in as " + event.getSelf().getUsername());
                });

        discordClient.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(this::isInFilmeChannel)
                .filter(App::isImdbLink)
                .subscribe(event -> {
                    try {
                        addMovie(event);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleException);

        discordClient.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(this::isInFilmeChannel)
                .filter(this::hasBotMention)
                .subscribe(event -> {
                    try {
                        suggestMovie(event);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleException);

        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(App::isThumbsUp)
                .map(event -> {
                    try {
                        return fetchUserIdAndImdbId(event);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .subscribe(userIdAndImdbId -> {
                    try {
                        handleLikeReaction(userIdAndImdbId.userId, userIdAndImdbId.imdbId);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleException);

        discordClient.getEventDispatcher().on(ReactionRemoveEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(App::isThumbsUp)
                .subscribe(event -> {
                    try {
                        handleUnlikeReaction(event);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleException);

        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(App::isEyesEmoji)
                .subscribe(event -> {
                    try {
                        handleMarkMovieAsSeenReaction(event);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, this::handleException);
    }

    public record UserIdAndImdbId(String userId, String imdbId) {
    }

    private Optional<UserIdAndImdbId> fetchUserIdAndImdbId(ReactionAddEvent event) throws SQLException {
        String imdbId = movieRepository.fetchImdbIdFromMessageId(event.getMessageId().asString())
                .orElse(null);
        if (imdbId == null) return Optional.empty();
        return Optional.of(new UserIdAndImdbId(event.getUserId().asString(), imdbId));
    }

    private static boolean isImdbLink(MessageCreateEvent event) {
        return event.getMessage().getContent().toLowerCase().contains("imdb.com/title/tt");
    }

    private boolean hasBotMention(MessageCreateEvent event) {
        return event.getMessage().getUserMentions().stream()
                .map(User::getId)
                .anyMatch(id -> id.equals(botSnowflake));
    }

    private boolean isInFilmeChannel(MessageCreateEvent event) {
        return event.getMessage().getChannelId().asLong() == movieChannelId;
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

    private boolean isReactionInMovieChannel(ReactionEvent event) {
        return event.getChannelId().asLong() == movieChannelId;
    }

    private String fetchMovieTitleFromOmdb(String imdbId) throws IOException, InterruptedException {
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

    private void addMovie(MessageCreateEvent event) throws IOException, InterruptedException, SQLException {
        String messageContent = event.getMessage().getContent();
        String messageId = event.getMessage().getId().asString();
        String imdbId = extractImdbId(messageContent);
        if (imdbId == null) {
            throw new IOException("Failed to extract IMDB ID from message: " + messageId);
        }
        String title = fetchMovieTitleFromOmdb(imdbId);
        boolean isOldMovie = movieRepository.persistMovie(imdbId, title);
        movieRepository.persistMessage(messageId, imdbId);
        String authorId = event.getMessage().getAuthor().get().getId().asString();
        movieRepository.persistLike(authorId, imdbId);

        if (isOldMovie) {
            IO.println("Movie already in database: " + title + " (" + imdbId + ")");
        } else {
            IO.println("Successfully persisted movie: " + title + " (" + imdbId + ")");
        }

        replaceImdbLinkWithShareImdbLink(event.getMessage());
    }

    private static void replaceImdbLinkWithShareImdbLink(Message message) {
        String messageContent = message.getContent();
        String newContent = messageContent.replaceAll("(?i)imdb\\.com", "shareimdb.com");
        if (!newContent.equals(messageContent)) {
            message.edit().withContentOrNull(newContent).subscribe();
        }
    }

    private void suggestMovie(MessageCreateEvent event) throws SQLException {
        List<String> mentionedUserIds = event.getMessage().getUserMentions()
                .stream().filter(user -> !user.isBot())
                .map(user -> user.getId().asString()).toList();

        if (mentionedUserIds.isEmpty()) return;

        ResultSet resultSet = movieRepository.fetchLikeCounts(mentionedUserIds);

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
        movieTitles.forEach(
                movieTitle -> channel.createMessage(movieTitle)
                        .map(Message::getId)
                        .map(Snowflake::asString)
                        .map(messageId -> {
                            try {
                                return movieRepository.fetchImdbIdFromTitle(messageId);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .subscribe(messageId -> {
                            try {
                                String imdbId = movieRepository.fetchImdbIdFromTitle(movieTitle);
                                movieRepository.persistMessage(messageId, imdbId);
                                IO.println("Persisted movie message: " + messageId + " (" + movieTitle + ")");
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }, this::handleException));
    }

    private void handleLikeReaction(String userId, String imdbId) throws SQLException {
        movieRepository.persistLike(userId, imdbId);
    }

    private void handleUnlikeReaction(ReactionRemoveEvent event) throws SQLException {
        String userId = event.getUserId().asString();
        String imdbId = null;
        imdbId = movieRepository.fetchImdbIdFromTitle(event.getMessageId().asString());
        movieRepository.deleteLike(imdbId, userId);
    }

    private void handleMarkMovieAsSeenReaction(ReactionAddEvent event) throws SQLException {
        String messageId = event.getMessageId().asString();
        String imdbId = null;
            imdbId = movieRepository.fetchImdbIdFromTitle(messageId);
        boolean isAlreadyMarkedAsSeen = false;
            isAlreadyMarkedAsSeen = movieRepository.markMovieAsSeen(imdbId);

        if (isAlreadyMarkedAsSeen) {
            IO.println("Movie already marked as seen: " + imdbId);
            return;
        }

        IO.println("Movie marked as seen: " + imdbId);

        List<String> messageIds = null;
        try {
            messageIds = movieRepository.fetchMovieIds(imdbId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Flux.fromIterable(messageIds)
                .map(Snowflake::of)
                .flatMap(snowflake -> discordClient.getMessageById(Snowflake.of(movieChannelId), snowflake))
                .subscribe(message -> message.addReaction(eyesEmoji).subscribe());
    }

    public void handleException(Throwable throwable) {
        String errorMessage = throwable.getMessage();
        System.err.println(errorMessage);

        discordClient.getChannelById(Snowflake.of(movieChannelId))
                .ofType(MessageChannel.class)
                .flatMap(channel -> channel.createMessage("⚠️ Error: " + errorMessage + " " + ownerMention))
                .subscribe();
    }
}