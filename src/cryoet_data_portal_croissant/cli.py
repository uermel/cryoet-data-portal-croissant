import click


@click.group()
@click.pass_context
def cli(ctx):
    """Command line interface for the cryoet_data_portal_croissant package."""
    pass


@cli.command()
@click.option(
    "--dataset_id",
    required=False,
    help="Dataset ID to generate the configuration for. If not provided, all datasets will be used.",
    type=int,
    multiple=True,
)
@click.pass_context
def generate(
    ctx,
    dataset_id: list[int] | None = None,
) -> None:
    """Generate a configuration file for the given dataset IDs."""

    from cryoet_data_portal_croissant.gen import generate_mlcroissant

    if dataset_id is None:
        dataset_id = []

    # Generate the mlcroissant structure graph
    generate_mlcroissant(dataset_id)


if __name__ == "__main__":
    cli()
