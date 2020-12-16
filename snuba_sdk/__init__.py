from abc import ABC, abstractmethod


class Expression(ABC):
    def __post_init__(self) -> None:
        self.validate()

    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def translate(self) -> str:
        raise NotImplementedError
