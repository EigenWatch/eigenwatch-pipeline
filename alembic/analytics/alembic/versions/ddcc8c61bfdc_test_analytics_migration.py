"""test analytics migration

Revision ID: ddcc8c61bfdc
Revises: 06528bca5156
Create Date: 2025-12-04 10:14:32.385540

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ddcc8c61bfdc'
down_revision: Union[str, Sequence[str], None] = '06528bca5156'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
