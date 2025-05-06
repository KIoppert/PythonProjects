# exception.py

class CompgraphException(Exception):
    """
    Exception raised when trying to run graph without operations
    """

    def __init__(self, msg: str) -> None:
        super().__init__(msg)
