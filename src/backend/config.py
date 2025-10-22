from core.config import Config, ConfigOpts
from core.constants import NeuronType


class BackendConfig(Config):
    def __init__(self):
        opts = ConfigOpts(
            neuron_name="backend",
            neuron_type=NeuronType.Validator,  # Backend uses validator-like chain access
            settings_files=["backend.toml"],
        )
        super().__init__(opts)

    def add_args(self):
        """Add command line arguments"""
        super().add_args()

        # database configuration
        self._parser.add_argument(
            "--database-url",
            type=str,
            help="PostgreSQL database URL for backend",
            default=self.settings.get(
                "database_url",
                "postgresql+asyncpg://postgres@localhost/kinitro_backend",
            ),
        )

        # websocket server configuration
        self._parser.add_argument(
            "--websocket-host",
            type=str,
            help="WebSocket server host to bind to",
            default=self.settings.get("websocket_host", "0.0.0.0"),
        )

        self._parser.add_argument(
            "--websocket-port",
            type=int,
            help="WebSocket server port to bind to",
            default=self.settings.get("websocket_port", 8080),
        )

        # chain monitoring configuration
        self._parser.add_argument(
            "--max-commitment-lookback",
            type=int,
            help="Maximum blocks to look back for commitments",
            default=self.settings.get("max_commitment_lookback", 360),
        )

        self._parser.add_argument(
            "--chain-sync-interval",
            type=int,
            help="Seconds between chain sync operations",
            default=self.settings.get("chain_sync_interval", 30),
        )

        self._parser.add_argument(
            "--validator-message-workers",
            # only allow positive integers, 0 or negative means use CPU-based default
            type=lambda x: int(x) if int(x) > 0 else 0,
            help="Number of validator message worker tasks (0 uses CPU-based default)",
            default=self.settings.get("validator_message_workers"),
        )
