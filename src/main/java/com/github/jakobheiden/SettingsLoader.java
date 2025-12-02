package com.github.jakobheiden;

import lombok.Getter;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Getter
public class SettingsLoader {
    private final String discordBotToken;
    private final String omdbApiKey;
    private final long movieChannelId;
    private final long testChannelId;
    private final long ownerId;
    private final long botId;

    public SettingsLoader(String filePath) {
        Yaml yaml = new Yaml();
        try (InputStream inputStream = new FileInputStream(filePath)) {
            Map<String, Object> data = yaml.load(inputStream);

            this.discordBotToken = requireNonNull(data, "discordBotToken");
            this.omdbApiKey = requireNonNull(data, "omdbApiKey");
            this.movieChannelId = requireLong(data, "movieChannelId");
            this.testChannelId = requireLong(data, "testChannelId");
            this.ownerId = requireLong(data, "ownerId");
            this.botId = requireLong(data, "botId");
        } catch (IOException e) {
            System.err.println("Could not load settings.yaml: " + e.getMessage());
            System.exit(1);
            throw new AssertionError("Unreachable");
        }
    }

    private static String requireNonNull(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) {
            System.err.println("Missing required setting: " + key);
            System.exit(1);
        }
        return (String) value;
    }

    private static long requireLong(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) {
            System.err.println("Missing required setting: " + key);
            System.exit(1);
        }
        return (long) value;
    }
}