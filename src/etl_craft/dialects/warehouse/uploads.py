"""Bound uploader expressions shared by warehouse actions."""

from etl_craft.core.uploads import UploadWriter


def writer_expression(uploads: tuple[UploadWriter, ...], column: str) -> str:
    """Attribute a staged row only to its admitted upload, with no fallback writer."""
    if not uploads:
        return ":updated_by"
    choices = " ".join(f"WHEN :upload_id_{i} THEN :upload_account_{i}" for i in range(len(uploads)))
    return f"CASE {column} {choices} END"


def writer_parameters(uploads: tuple[UploadWriter, ...]) -> dict[str, object]:
    """Keep both upload identities and account names out of generated SQL text."""
    return {
        key: value
        for i, upload in enumerate(uploads)
        for key, value in (
            (f"upload_id_{i}", upload.upload_id),
            (f"upload_account_{i}", upload.account),
        )
    }


def invalid_upload_rows(stage: str, uploads: tuple[UploadWriter, ...]) -> str:
    """Locate unowned rows before an action can mutate its target."""
    ids = ", ".join(f":upload_id_{i}" for i in range(len(uploads)))
    return f"SELECT UPLOAD_ID FROM {stage} WHERE UPLOAD_ID IS NULL OR UPLOAD_ID NOT IN ({ids})"


def actor_parts(uploads: tuple[UploadWriter, ...], column: str) -> tuple[str, str]:
    """Keep ordinary append and overwrite account columns aligned with their values."""
    if not uploads:
        return "", ""
    writer = writer_expression(uploads, column)
    return ", CREATED_BY, UPDATED_BY", f", {writer}, {writer}"


def upload_select(sql: str, count: int, *, validating: bool) -> str:
    """Expand only executable upload tokens into bound parameter names."""
    import re

    from etl_craft.core.errors import SqlGuardError
    from etl_craft.core.text import sql_parts

    parts = list(sql_parts(sql, placeholders=True))
    token = re.compile(r"\$\$upload_ids\b")
    if any(not protected and token.search(part) for part, protected in parts):
        if not count and not validating:
            raise SqlGuardError(
                "$$upload_ids has no admitted uploads; expected an upload-bound run; "
                "upload a corrected file"
            )
        names = "NULL" if validating else ", ".join(f":upload_id_{i}" for i in range(count))
        return "".join(part if protected else token.sub(names, part) for part, protected in parts)
    return sql
