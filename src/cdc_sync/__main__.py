import asyncio
import sys

from cdc_sync.app import Application


def main() -> None:
    app = Application()
    exit_code = asyncio.run(app.run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
