from __future__ import annotations

import time

from rot.app.main import main


def loop(interval_s: int = 30) -> None:
    while True:
        main()
        time.sleep(interval_s)


if __name__ == "__main__":
    loop()
