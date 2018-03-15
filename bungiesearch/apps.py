from django.apps import AppConfig


class BungiesearchConfig(AppConfig):
    name = 'bungiesearch'
    verbose_name = "Bungiesearch"

    def ready(self):
        from . import Bungiesearch
        Bungiesearch.__load_settings__()
