from abc import ABC, abstractmethod


class Expression(ABC):
    @abstractmethod
    def validate(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def translate(self) -> str:
        raise NotImplementedError
