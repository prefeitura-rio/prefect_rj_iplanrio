from abc import ABC, abstractmethod
import pandas as pd


class TransformStrategy(ABC):
    """Interface base para todas as transformações."""

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa a transformação e retorna o novo DataFrame."""
        pass
