"""
Command pattern interfaces for the fire recovery backend.
"""

from .command import Command
from .command_context import CommandContext
from .command_result import CommandResult

__all__ = ["Command", "CommandContext", "CommandResult"]