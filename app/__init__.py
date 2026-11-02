from __future__ import annotations

from flask import Flask

from app.config import SETTINGS_CONFIG_KEY, Settings, configure_logging
from app.database import init_db
from app.routes import api, register_error_handlers


def create_app(settings: Settings | None = None) -> Flask:
    settings = settings or Settings.from_env()
    configure_logging(settings.log_level)

    app = Flask(__name__)
    app.config[SETTINGS_CONFIG_KEY] = settings
    app.config["MAX_CONTENT_LENGTH"] = settings.max_content_length

    init_db(settings)
    app.register_blueprint(api)
    register_error_handlers(app)
    return app