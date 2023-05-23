import shortuuid


def log_exception(message, exception):
    print(message, str(exception))


def generate_short_uuid():
    return shortuuid.uuid()
