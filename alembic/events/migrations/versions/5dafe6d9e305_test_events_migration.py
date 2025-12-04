"""test events migration

Revision ID: 5dafe6d9e305
Revises: 414ba4028ceb
Create Date: 2025-12-04 10:13:56.435059

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5dafe6d9e305'
down_revision: Union[str, Sequence[str], None] = '414ba4028ceb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
