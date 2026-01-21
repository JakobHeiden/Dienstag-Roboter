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
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
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
    private static final UnicodeEmoji thumbsUpEmoji = UnicodeEmoji.of("\uD83D\uDC4D");
    private static final UnicodeEmoji resetEmoji = UnicodeEmoji.of("\uD83D\uDD04");
    private static final String OMDB_API_URL_TEMPLATE = "https://www.omdbapi.com/?apikey=%s&i=%s";
    private static final Pattern IMDB_ID_PATTERN = Pattern.compile("imdb\\.com/(?:[a-z]{2}/)?title/(tt\\d+)", Pattern.CASE_INSENSITIVE);

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
        Hooks.onErrorDropped(this::handleException);
        configureEventHandlers();
        discordClient.onDisconnect().block();
    }

    private void configureEventHandlers() {
        // on login
        discordClient.getEventDispatcher().on(ReadyEvent.class)
                .subscribe(event -> {
                    IO.println("Bot logged in as " + event.getSelf().getUsername());
                });

        // persist a movie
        discordClient.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(this::isInFilmeChannel)
                .filter(App::isImdbLink)
                .flatMap(event -> Mono.fromCallable(() -> {
                    addMovie(event);
                    return null;
                }))
                .subscribe(null, this::handleException);

        // suggest movies
        discordClient.getEventDispatcher().on(MessageCreateEvent.class)
                .filter(this::isInFilmeChannel)
                .filter(this::hasBotMention)
                .flatMap(event -> Mono.fromCallable(() -> {
                    suggestMovies(event);
                    return null;
                }))
                .subscribe(null, this::handleException);

        // leave a like
        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(event -> !event.getMember().get().isBot())
                .filter(App::isThumbsUp)
                .flatMap(event -> Mono.fromCallable(() -> fetchUserIdAndImdbId(event))
                        .flatMap(Mono::justOrEmpty))
                .flatMap(userIdAndImdbId ->
                    Mono.fromCallable(() -> {
                        handleLikeReaction(userIdAndImdbId.userId, userIdAndImdbId.imdbId);
                        return null;
                    })
                )
                .subscribe(null, this::handleException);

        // remove a like
        discordClient.getEventDispatcher().on(ReactionRemoveEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(App::isThumbsUp)
                .flatMap(event -> Mono.fromCallable(() -> {
                    handleUnlikeReaction(event);
                    return null;
                }))
                .subscribe(null, this::handleException);

        // mark as seen
        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(App::isEyesEmoji)
                .flatMap(event ->
                    Mono.fromCallable(() -> movieRepository.fetchImdbIdFromMessageId(event.getMessageId().asString()))
                        .flatMap(Mono::justOrEmpty)
                )
                .flatMap(imdbId ->
                    Mono.fromCallable(() -> {
                        handleMarkMovieAsSeenReaction(imdbId);
                        return null;
                    })
                )
                .subscribe(null, this::handleException);

        // mark as not seen
        discordClient.getEventDispatcher().on(ReactionAddEvent.class)
                .filter(this::isReactionInMovieChannel)
                .filter(App::isResetEmoji)
                .flatMap(event ->
                    Mono.fromCallable(() -> movieRepository.fetchImdbIdFromMessageId(event.getMessageId().asString()))
                        .flatMap(Mono::justOrEmpty)
                )
                .flatMap(imdbId ->
                    Mono.fromCallable(() -> {
                        handleMarkMovieAsNotSeenReaction(imdbId);
                        return null;
                    })
                )
                .subscribe(null, this::handleException);
    }

    private record UserIdAndImdbId(String userId, String imdbId) {
    }

    private Optional<UserIdAndImdbId> fetchUserIdAndImdbId(ReactionAddEvent event) throws SQLException {
        String imdbId = movieRepository.fetchImdbIdFromMessageId(event.getMessageId().asString())
                .orElse(null);
        if (imdbId == null) return Optional.empty();
        return Optional.of(new UserIdAndImdbId(event.getUserId().asString(), imdbId));
    }

    private static boolean isImdbLink(MessageCreateEvent event) {
        return IMDB_ID_PATTERN.matcher(event.getMessage().getContent()).find();
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

    private static boolean isResetEmoji(ReactionAddEvent event) {
        return event.getEmoji().equals(resetEmoji);
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

    private String[] fetchMovieTitleAndYearFromOmdb(String imdbId) throws IOException, InterruptedException {
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

        return new String[]{json.get("Title").getAsString(), json.get("Year").getAsString()};
    }

    private static String extractImdbId(String message) {
        Matcher matcher = IMDB_ID_PATTERN.matcher(message);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private void addMovie(MessageCreateEvent event) throws IOException, SQLException {
        String messageContent = event.getMessage().getContent();
        String messageId = event.getMessage().getId().asString();
        String imdbId = extractImdbId(messageContent);
        if (imdbId == null) {
            throw new IOException("Failed to extract IMDB ID from message: " + messageId);
        }
        String[] titleAndYear;
        try {
            titleAndYear = fetchMovieTitleAndYearFromOmdb(imdbId);
        } catch (Exception e) {
            event.getMessage().getChannel().block().createMessage("Failed to extract title from OMDB API: " + e.getMessage() +
                    "\nMovie not persisted in database.").subscribe();
            return;
        }
        boolean isOldMovie = movieRepository.persistMovie(imdbId, titleAndYear[0], titleAndYear[1]);
        movieRepository.persistMessage(messageId, imdbId);
        String authorId = event.getMessage().getAuthor().get().getId().asString();
        movieRepository.persistLike(authorId, imdbId);

        if (isOldMovie) {
            IO.println("Movie already in database: " + titleAndYear[0] + " (" + imdbId + ")");
        } else {
            IO.println("Successfully persisted movie: " + titleAndYear[0] + " (" + imdbId + ")");
        }
        event.getMessage().addReaction(thumbsUpEmoji).subscribe();
    }

    private void suggestMovies(MessageCreateEvent event) throws SQLException {
        List<String> mentionedUserIds = event.getMessage().getUserMentions()
                .stream().filter(user -> !user.isBot())
                .map(user -> user.getId().asString()).toList();

        if (mentionedUserIds.isEmpty()) return;

        MovieRepository.MovieSuggestions movieSuggestions = movieRepository.fetchMovieSuggestions(mentionedUserIds);

        if (movieSuggestions.maxTaggedLikeCount() == 0) {
            IO.println("No movies to suggest");
            event.getMessage().getChannel().subscribe(messageChannel ->
                    messageChannel.createMessage("No movies to suggest").subscribe());
            return;
        }

        IO.println(String.format("Suggesting %d titles", movieSuggestions.titles().size()));

        MessageChannel channel = event.getMessage().getChannel().block();
        for (int i = 0; i < movieSuggestions.titles().size(); i++) {
            String imdbId = movieSuggestions.imdbIds().get(i);
            String title = movieSuggestions.titles().get(i);
            String year = movieSuggestions.years().get(i);
            channel.createMessage(String.format("%d/%d %s%s", movieSuggestions.maxTaggedLikeCount(),
                            movieSuggestions.allLikeCounts().get(i),
                            title,
                            year != null ? " (" + year + ")" : ""))
                    .map(Message::getId)
                    .map(Snowflake::asString)
                    .flatMap(messageId ->
                            Mono.fromCallable(() -> {
                                movieRepository.persistMessage(messageId, imdbId);
                                IO.println("Persisted movie message: " + messageId + " (" + imdbId + ")");
                                return null;
                            }))
                    .subscribe(null, this::handleException);
        }
   }

    private void handleLikeReaction(String userId, String imdbId) throws SQLException {
        movieRepository.persistLike(userId, imdbId);
    }

    private void handleUnlikeReaction(ReactionRemoveEvent event) throws SQLException {
        String userId = event.getUserId().asString();
        Optional<String> maybeImdbId = movieRepository.fetchImdbIdFromMessageId(event.getMessageId().asString());
        if (maybeImdbId.isEmpty()) return;

        movieRepository.deleteLike(maybeImdbId.get(), userId);
    }

    private void handleMarkMovieAsSeenReaction(String imdbId) throws SQLException {
        boolean isAlreadyMarkedAsSeen = movieRepository.markMovieAsSeen(imdbId);
        if (isAlreadyMarkedAsSeen) {
            IO.println("Movie already marked as seen: " + imdbId);
            return;
        }

        IO.println("Movie marked as seen: " + imdbId);

        List<String> messageIds = movieRepository.fetchMessageIds(imdbId);
        Flux.fromIterable(messageIds)
                .map(Snowflake::of)
                .flatMap(snowflake -> discordClient.getMessageById(Snowflake.of(movieChannelId), snowflake))
                .flatMap(message -> message.addReaction(eyesEmoji))
                .subscribe(null, this::handleException);
    }

    private void handleMarkMovieAsNotSeenReaction(String imdbId) throws SQLException {
        boolean isAlreadyMarkedAsNotSeen = movieRepository.markMovieAsNotSeen(imdbId);
        if (isAlreadyMarkedAsNotSeen) {
            IO.println("Movie already marked as not seen: " + imdbId);
            return;
        }

        IO.println("Movie marked as not seen: " + imdbId);

        List<String> messageIds = movieRepository.fetchMessageIds(imdbId);
        Flux.fromIterable(messageIds)
                .map(Snowflake::of)
                .flatMap(snowflake -> discordClient.getMessageById(Snowflake.of(movieChannelId), snowflake))
                .flatMap(message -> message.removeReaction(eyesEmoji, botSnowflake))
                .subscribe(null, this::handleException);
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