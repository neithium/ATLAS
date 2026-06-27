import json

from datetime import datetime

# =========================================================
# ERROR TYPES
# =========================================================

RECOVERABLE_ERRORS = [

    "INVALID_SCHEMA",

    "INVALID_SOCKET_COUNT"
]

NON_RECOVERABLE_ERRORS = [

    "MISSING_DEVICE_ID",

    "MISSING_POWERDETAIL"
]

# =========================================================
# SOCKET COUNT FIX
# =========================================================

def fix_socket_count(record):

    try:

        inventory = record.get(
            "inventory_data"
        )

        if inventory:

            socket_count = inventory.get(
                "socket_count"
            )

            if isinstance(socket_count, str):

                inventory["socket_count"] = int(
                    socket_count
                )

        return True, record

    except Exception as e:

        print(
            f"❌ socket fix failed: {e}"
        )

        return False, record

# =========================================================
# TIMESTAMP FIX
# =========================================================

def normalize_timestamp(record):

    try:

        created_at = record.get(
            "created_at"
        )

        if created_at:

            if "/" in created_at:

                fixed = datetime.strptime(

                    created_at,

                    "%d/%m/%Y %H:%M:%S"
                )

                record["created_at"] = (
                    fixed.isoformat()
                )

        return True, record

    except Exception as e:

        print(
            f"❌ timestamp fix failed: {e}"
        )

        return False, record

# =========================================================
# MAIN RECOVERY ENGINE
# =========================================================

def recover_record(dlq_message):

    try:

        error_type = dlq_message[
            "error_type"
        ]

        raw_json = dlq_message[
            "raw_json"
        ]

        record = json.loads(raw_json)

        # -------------------------------------------------
        # NON RECOVERABLE
        # -------------------------------------------------

        if error_type in NON_RECOVERABLE_ERRORS:

            return False, record

        # -------------------------------------------------
        # RECOVERABLE
        # -------------------------------------------------

        success = True

        if error_type in [

            "INVALID_SOCKET_COUNT",

            "INVALID_SCHEMA"
        ]:

            success, record = fix_socket_count(
                record
            )

        success, record = normalize_timestamp(
            record
        )

        # -------------------------------------------------
        # METADATA
        # -------------------------------------------------

        record["recovery_metadata"] = {

            "reviewed_by":
                "DLQ_REVIEWER_V1",

            "recovery_type":
                error_type,

            "recovered_at":
                str(datetime.utcnow())
        }

        return success, record

    except Exception as e:

        print(
            f"❌ recovery failed: {e}"
        )

        return False, {}