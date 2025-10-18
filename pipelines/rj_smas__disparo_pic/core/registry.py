# -*- coding: utf-8 -*-
class TransformerRegistry:
    """Registro global de transformações."""

    _registry = {}

    @classmethod
    def register(cls, name):
        """Decorator para registrar uma classe de transformação."""

        def wrapper(klass):
            cls._registry[name] = klass
            return klass

        return wrapper

    @classmethod
    def create(cls, name, **kwargs):
        """Instancia a transformação a partir do nome registrado."""
        if name not in cls._registry:
            raise ValueError(f"Transformer '{name}' not found in registry")
        return cls._registry[name](**kwargs)
