
class ValidationError(Exception):
    def __init__(self, message):
        self.message = message

class ManifestNotFoundError(Exception):

    def __init__(self, message):
        self.message = message
