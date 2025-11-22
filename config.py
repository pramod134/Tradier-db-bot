import os
from dataclasses import dataclass


@dataclass
class Settings:
    tradier_token: str
    tradier_accounts: list[str]
    tradier_base: str
    supabase_url: str
    supabase_key: str
    poll_positions_sec: int
    poll_quotes_sec: int

    @classmethod
    def load(cls) -> "Settings":
        return cls(
            tradier_token=os.environ["TRADIER_TOKEN"],
            tradier_accounts=[
                a.strip()
                for a in os.environ["TRADIER_ACCOUNT_IDS"].split(",")
                if a.strip()
            ],
            tradier_base=os.environ.get(
                "TRADIER_BASE_URL", "https://api.tradier.com/v1"
            ),
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_key=os.environ["SUPABASE_SERVICE_KEY"],
            poll_positions_sec=int(os.environ.get("POLL_POSITIONS_SEC", 10)),
            poll_quotes_sec=int(os.environ.get("POLL_QUOTES_SEC", 5)),
        )


settings = Settings.load()
