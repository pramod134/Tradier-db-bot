import asyncio

from .loops import run_positions_loop, run_quotes_loop


async def main() -> None:
    await asyncio.gather(
        run_positions_loop(),
        run_quotes_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
