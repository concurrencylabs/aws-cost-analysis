
class ValidationError(Exception):
    def __init__(self, message):
        self.message = message

class CurBucketNotFoundError(Exception):
    def __init__(self, message):
        self.message = message


class ManifestNotFoundError(Exception):
    def __init__(self, message):
        self.message = message

class AthenaExecutionFailedException(Exception):
    def __init__(self, message):
        self.message = message

class AwsPayerAccountNotFoundError(Exception):
    def __init__(self, message):
        self.message = message

