# ptvtracker-data Logging Example

This Go project demonstrates logging using four different libraries:

- **Zerolog**: Fast, structured, leveled logging for Go.
- **Lumberjack**: Rolling log file writer, used here as a file backend for Zerolog.
- **Logy**: Fast, configurable, and easy-to-use logger for Go applications.
- **Logdy**: Real-time log viewer with web UI, can be used as a Go library for log streaming.

## Setup

1. Ensure you have Go installed (1.18+ recommended).
2. Install dependencies:

    ```sh
    go mod tidy
    ```

3. Run the example:

    ```sh
    go run main.go
    ```

## What Happens

- **Zerolog** logs a message to the console.
- **Lumberjack** writes a log message to a file (`lumberjack.log`) with rotation enabled.
- **Logy** logs to the console and demonstrates contextual logging.
- **Logdy** starts a log stream (by default, to localhost:8080) and logs messages that can be viewed in a web UI if you run the Logdy binary.

## Notes
- For Logdy's web UI, you may need to run the Logdy binary separately and visit [http://localhost:8080](http://localhost:8080).
- All libraries are included via Go modules.

## References
- [Zerolog](https://github.com/rs/zerolog)
- [Lumberjack](https://github.com/natefinch/lumberjack)
- [Logy](https://github.com/codnect/logy)
- [Logdy](https://github.com/logdyhq/logdy-core) 