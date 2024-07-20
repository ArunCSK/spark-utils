from abc import ABC, abstractmethod


class CONF(ABC):

    def __init__(self) -> None:
        self.s3_bucket = ""

    