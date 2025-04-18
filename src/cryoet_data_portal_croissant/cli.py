import click


@click.group()
@click.pass_context
def cli(ctx):
    """Command line interface for the cryoet_data_portal_croissant package."""
    pass


@cli.command()
@click.option("--dataset_ids", required=False, help="Comma-separated list of dataset IDs.")
@click.pass_context
def generate_config(ctx,
                    dataset_ids,
                    ):
    """Generate a configuration file for the given dataset IDs."""


if __name__ == "__main__":
    cli()
