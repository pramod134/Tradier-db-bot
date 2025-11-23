import os
from dataclasses import dataclass


@dataclass
class Settings:
    # Sandbox (positions)
    tradier_sandbox_token: str
    tradier_sandbox_accounts: list[str]
    tradier_sandbox_base: str

    # Live (quotes)
    tradier_live_token: str
    tradier_live_base: str

    # Supabase
    supabase_url: str
    supabase_key: str

    # Timers
    poll_positions_sec: int
    poll_quotes_sec: int

    poll_spot_tf_sec: int = int(os.getenv("POLL_SPOT_TF_SEC", "900"))  # 15 minutes default

    @classmethod
    def load(cls) -> "Settings":
        return cls(
            # Sandbox
            tradier_sandbox_token=os.environ["TRADIER_SANDBOX_TOKEN"],
            tradier_sandbox_accounts=[
                a.strip()
                for a in os.environ["TRADIER_SANDBOX_ACCOUNT_IDS"].split(",")
                if a.strip()
            ],
            tradier_sandbox_base=os.environ.get(
                "TRADIER_SANDBOX_BASE_URL", "https://sandbox.tradier.com/v1"
            ),

            # Live
            tradier_live_token=os.environ["TRADIER_LIVE_TOKEN"],
            tradier_live_base=os.environ.get(
                "TRADIER_LIVE_BASE_URL", "https://api.tradier.com/v1"
            ),

            # Supabase
            supabase_url=os.environ["SUPABASE_URL"],
            supabase_key=os.environ["SUPABASE_SERVICE_KEY"],

            # Timers
            poll_positions_sec=int(os.environ.get("POLL_POSITIONS_SEC", 10)),
            poll_quotes_sec=int(os.environ.get("POLL_QUOTES_SEC", 5)),
        )


settings = Settings.load()
