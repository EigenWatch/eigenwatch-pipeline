from typing import List, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
import dagster as dg

from subgraph_pipeline.models.entities import (
    Operator,
    Staker,
    AVS,
    Strategy,
    OperatorSet,
    EigenPod,
)


class EntityManager(dg.ConfigurableResource):
    """
    Unified entity manager with generic upsert for simple address-based entities.
    """

    # =================================================================== #
    # GENERIC: Simple address-based entities (Operator, Staker, AVS, Strategy)
    # =================================================================== #
    def _upsert_simple(
        self,
        session: Session,
        model: Any,
        entity_ids: List[str],
        context: dg.OpExecutionContext = None,
    ) -> Dict[str, int]:
        """
        Generic upsert for entities where:
        - PK = `id` (string)
        - `address` = `id`
        - No foreign keys
        """
        if not entity_ids:
            return {"inserted": 0, "updated": 0, "skipped": 0}

        unique_ids = list(set(entity_ids))
        inserted = updated = skipped = 0
        now = datetime.now(timezone.utc)

        for entity_id in unique_ids:
            try:
                stmt = (
                    insert(model)
                    .values(
                        id=entity_id,
                        address=entity_id,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=["id"],
                        set_={"updated_at": now},
                    )
                    .returning(
                        model.id,
                        model.created_at,
                        model.updated_at,
                    )
                )

                result = session.execute(stmt)
                row = result.fetchone()

                if row:
                    # Access by index instead of attribute names
                    if row[1] == row[2]:  # created_at == updated_at
                        inserted += 1
                    else:
                        updated += 1
                else:
                    skipped += 1

            except Exception as e:
                if context:
                    context.log.warning(
                        f"Failed to upsert {model.__tablename__} {entity_id}: {e}"
                    )
                skipped += 1
                continue

        if context:
            context.log.info(
                f"{model.__name__} upsert: {inserted} inserted, "
                f"{updated} updated, {skipped} skipped out of {len(unique_ids)}"
            )

        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    # =================================================================== #
    # Public wrappers — clean API, no duplication
    # =================================================================== #
    def upsert_operators(self, session: Session, operator_ids: List[str], context=None):
        return self._upsert_simple(session, Operator, operator_ids, context)

    def upsert_stakers(self, session: Session, staker_ids: List[str], context=None):
        return self._upsert_simple(session, Staker, staker_ids, context)

    def upsert_avs(self, session: Session, avs_ids: List[str], context=None):
        return self._upsert_simple(session, AVS, avs_ids, context)

    def upsert_strategies(
        self, session: Session, strategy_ids: List[str], context=None
    ):
        return self._upsert_simple(session, Strategy, strategy_ids, context)

    # =================================================================== #
    # SPECIAL: OperatorSet (composite key)
    # =================================================================== #
    def upsert_operator_sets(
        self,
        session: Session,
        operator_set_data: List[Dict[str, Any]],
        context: dg.OpExecutionContext = None,
    ) -> Dict[str, int]:
        if not operator_set_data:
            return {"inserted": 0, "updated": 0, "skipped": 0}

        inserted = updated = skipped = 0
        now = datetime.now(timezone.utc)

        # 1️⃣ Extract all unique avs_ids first
        avs_ids = list(
            {entry.get("avs_id") for entry in operator_set_data if entry.get("avs_id")}
        )
        if avs_ids:
            try:
                # 2️⃣ Ensure all parent AVS rows exist before inserting operator sets
                self.upsert_avs(session, avs_ids, context)
                session.flush()  # make sure they’re visible in the same transaction
            except Exception as e:
                if context:
                    context.log.warning(
                        f"Failed to pre-upsert AVS for operator sets: {e}"
                    )

        # 3️⃣ Proceed with operator_set upserts as before
        for entry in operator_set_data:
            avs_id = entry.get("avs_id")
            op_set_id = entry.get("operator_set_id")
            composite_id = entry.get("id") or f"{avs_id}-{op_set_id}"

            if not avs_id or op_set_id is None:
                if context:
                    context.log.warning(f"Invalid operator set data: {entry}")
                skipped += 1
                continue

            try:
                stmt = (
                    insert(OperatorSet)
                    .values(
                        id=composite_id,
                        avs_id=avs_id,
                        operator_set_id=op_set_id,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=["id"],
                        set_={"updated_at": now},
                    )
                    .returning(
                        OperatorSet.id,
                        OperatorSet.created_at,
                        OperatorSet.updated_at,
                    )
                )
                result = session.execute(stmt)
                row = result.fetchone()

                if row:
                    if row[1] == row[2]:  # created_at == updated_at
                        inserted += 1
                    else:
                        updated += 1
                else:
                    skipped += 1

            except Exception as e:
                if context:
                    context.log.warning(
                        f"Failed to upsert operator set {composite_id}: {e}"
                    )
                skipped += 1
                continue

        if context:
            context.log.info(
                f"OperatorSet upsert: {inserted} inserted, {updated} updated, "
                f"{skipped} skipped out of {len(operator_set_data)}"
            )
        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    # =================================================================== #
    # SPECIAL: EigenPod (FK to Staker)
    # =================================================================== #
    def upsert_eigen_pods(
        self,
        session: Session,
        pod_data: List[Dict[str, str]],
        context: dg.OpExecutionContext = None,
    ) -> Dict[str, int]:
        if not pod_data:
            return {"inserted": 0, "updated": 0, "skipped": 0}

        inserted = updated = skipped = 0
        now = datetime.now(timezone.utc)

        # Extract all unique stacker_ids first
        owner_ids = list(
            {entry.get("owner_id") for entry in pod_data if entry.get("owner_id")}
        )
        if owner_ids:
            try:
                # Ensure all parent staker rows exist before inserting eigen pod
                self.upsert_stakers(session, owner_ids, context)
                session.flush()  # make sure they’re visible in the same transaction
            except Exception as e:
                if context:
                    context.log.warning(
                        f"Failed to pre-upsert Stackers for Eigen Pods: {e}"
                    )

        for entry in pod_data:
            pod_address = entry.get("address")
            owner_id = entry.get("owner_id")
            pod_id = entry.get("id") or pod_address

            if not pod_address or not owner_id:
                if context:
                    context.log.warning(
                        f"Invalid EigenPod data (missing address/owner): {entry}"
                    )
                skipped += 1
                continue

            try:
                stmt = (
                    insert(EigenPod)
                    .values(
                        id=pod_id,
                        address=pod_address,
                        owner_id=owner_id,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "owner_id": owner_id,
                            "updated_at": now,
                        },
                        where=(EigenPod.owner_id != owner_id),
                    )
                    .returning(
                        EigenPod.id,
                        EigenPod.created_at,
                        EigenPod.updated_at,
                    )
                )
                result = session.execute(stmt)
                row = result.fetchone()

                if row:
                    # Access by index
                    if row[1] == row[2]:  # created_at == updated_at
                        inserted += 1
                    else:
                        updated += 1
                else:
                    skipped += 1

            except Exception as e:
                if context:
                    context.log.warning(f"Failed to upsert EigenPod {pod_id}: {e}")
                skipped += 1
                continue

        if context:
            context.log.info(
                f"EigenPod upsert: {inserted} inserted, {updated} updated, "
                f"{skipped} skipped out of {len(pod_data)}"
            )
        return {"inserted": inserted, "updated": updated, "skipped": skipped}
