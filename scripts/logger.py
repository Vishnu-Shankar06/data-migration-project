def log_error(message):
    with open("logs/errors.log", "a") as f:
        f.write(message + "\n")