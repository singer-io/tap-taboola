class TaboolaForbiddenError(Exception):
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class TaboolaUnauthorizedError(Exception):
    """Raised when credentials are invalid or a token cannot be obtained.

    Covers both HTTP 401 responses from authenticated requests and
    token-generation failures (invalid client_id/client_secret/username/
    password), so callers can distinguish invalid-credential failures from
    other, potentially recoverable, errors (e.g. forbidden/403 per-stream
    access issues).
    """
    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response
