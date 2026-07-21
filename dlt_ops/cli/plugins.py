import click

from dlt_ops.plugins import registry


@click.group()
def plugins() -> None:
    """Inspect the plugin registry."""


@plugins.command()
def doctor() -> None:
    """Report every plugin axis: registered plugins, load failures, collisions.

    Exits 0 when every discovered plugin loads cleanly and no ``<axis>/<name>``
    is contested; exits 1 otherwise (CI-usable).
    """
    failure_count = 0
    collided = {(collision.axis, collision.name): collision for collision in registry.collisions()}

    for axis in registry.AXES:
        names = registry.names(axis)
        if not names:
            click.echo(f"{axis}: (none)")
            continue
        click.echo(f"{axis}:")
        for name in names:
            collision = collided.get((axis, name))
            if collision is not None:
                claimants = ", ".join(f"{source.label!r} ({source.value})" for source in collision.sources)
                click.echo(f"  {name}  COLLISION: {claimants}")
                click.echo("  disambiguate in .dlt/config.toml:")
                click.echo(collision.disambiguation_toml())
                continue
            source = registry.source(axis, name)
            origin = f"[{source.label}]  {source.value}" if source is not None else ""
            try:
                registry.get(axis, name)
            except Exception as exc:
                failure_count += 1
                click.echo(f"  {name}  {origin}  FAILED: {type(exc).__name__}: {exc}")
                continue
            click.echo(f"  {name}  {origin}")

    if failure_count or collided:
        click.echo(f"plugins doctor: {failure_count} failure(s), {len(collided)} collision(s)")
        raise SystemExit(1)
    click.echo("plugins doctor: OK")
