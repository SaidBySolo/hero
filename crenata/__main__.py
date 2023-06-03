from argparse import ArgumentParser
from sys import argv

from discord import Intents, Object

from crenata.argparser import parse_args
from crenata.config import CrenataConfig
from crenata.discord.client import create_client
from crenata.discord.commands import commands
from crenata.discord.events.error import on_error

if __name__ == "__main__":
    config = CrenataConfig()
    parser = ArgumentParser("crenata")
    args = parse_args(parser, argv[1:])
    config.update_with_args(args)

    client = create_client(config, intents=Intents.default())
    for command in commands:
        if config.PRODUCTION:
            client.tree.add_command(command)  # type: ignore
        else:
            client.tree.add_command(command, guild=Object(config.TEST_GUILD_ID))  # type: ignore
    setattr(client.tree, "on_error", on_error)
    client.run()
