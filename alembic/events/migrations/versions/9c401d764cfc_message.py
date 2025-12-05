"""message

Revision ID: 9c401d764cfc
Revises: 5dafe6d9e305
Create Date: 2025-12-05 12:15:03.245417

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c401d764cfc'
down_revision: Union[str, Sequence[str], None] = '5dafe6d9e305'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
