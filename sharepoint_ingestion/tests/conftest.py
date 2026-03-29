"""Shared pytest fixtures."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def spark():
    return MagicMock()
