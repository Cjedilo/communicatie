from communicatie import __version__
from communicatie.config import DEFAULT_PORT, Config


def test_package_imports():
    assert __version__


def test_config_defaults():
    config = Config.from_env(env={})
    assert config.port == DEFAULT_PORT
    assert config.database_url.startswith("postgresql://")


def test_config_reads_env():
    config = Config.from_env(
        env={
            "COMMUNICATIE_HOST": "127.0.0.1",
            "COMMUNICATIE_PORT": "9000",
            "COMMUNICATIE_DATABASE_URL": "postgresql://u:p@db/x",
        }
    )
    assert config.host == "127.0.0.1"
    assert config.port == 9000
    assert config.database_url == "postgresql://u:p@db/x"
