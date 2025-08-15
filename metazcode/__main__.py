from dotenv import load_dotenv
from metazcode.cli.commands import cli


def main():
    # Load environment variables from .env file
    load_dotenv()
    cli()


if __name__ == "__main__":
    main()
