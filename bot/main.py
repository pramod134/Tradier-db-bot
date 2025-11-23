import asyncio

from .loops import (
    run_positions_loop,
    run_quotes_loop,
    run_spot_indicators_loop,
)


async def main() -> None:
    await asyncio.gather(
        run_positions_loop(),
        run_quotes_loop(),
        run_spot_indicators_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
