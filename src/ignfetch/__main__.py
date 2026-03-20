from .main import app
from .utils import setup_logging


def main():
    setup_logging()
    app()


if __name__ == "__main__":
    main()
