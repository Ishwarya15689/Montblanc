from common_utils.cmd_provider import Parameter


class MontBlancParameter(Parameter):
    def __init__(self):
        super().__init__()
        self.parser.add_argument(
            "--montblanc_outbounddb_secret_name",
            required=True,
            help="Montblanc outbound db secret name",
        )

    def get_montblanc_outbound_db_secret_name(self):
        return self.parser.parse_args().montblanc_outbounddb_secret_name
