#!/usr/bin/env python3
"""Upload one file to Google Drive (OAuth desktop flow + token reuse)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]


def _load_creds(client_secrets: Path, token_file: Path) -> Credentials:
    creds = None
    if token_file.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_file), SCOPES)
        except Exception:
            creds = None

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        if not client_secrets.exists():
            raise RuntimeError(f"client_secrets missing: {client_secrets}")
        flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets), SCOPES)
        creds = flow.run_local_server(port=0, open_browser=False)

    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(creds.to_json(), encoding="utf-8")
    return creds


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload a file to Google Drive")
    ap.add_argument("--file", required=True)
    ap.add_argument("--client-secrets", required=True)
    ap.add_argument("--token-file", required=True)
    ap.add_argument("--name", default="")
    ap.add_argument("--folder-id", default="")
    ap.add_argument("--anyone-reader", action="store_true")
    args = ap.parse_args()

    src = Path(args.file).expanduser().resolve()
    if not src.exists() or not src.is_file():
        raise SystemExit(json.dumps({"status": "error", "error": f"file_not_found:{src}"}, ensure_ascii=False))

    client_secrets = Path(args.client_secrets).expanduser().resolve()
    token_file = Path(args.token_file).expanduser().resolve()

    creds = _load_creds(client_secrets, token_file)
    service = build("drive", "v3", credentials=creds)

    metadata = {"name": args.name or src.name}
    if args.folder_id.strip():
        metadata["parents"] = [args.folder_id.strip()]

    media = MediaFileUpload(str(src), resumable=True)
    file = (
        service.files()
        .create(body=metadata, media_body=media, fields="id,name,webViewLink,webContentLink,parents")
        .execute()
    )

    if args.anyone_reader:
        service.permissions().create(
            fileId=file["id"],
            body={"type": "anyone", "role": "reader"},
            fields="id",
        ).execute()
        file = (
            service.files()
            .get(fileId=file["id"], fields="id,name,webViewLink,webContentLink,parents")
            .execute()
        )

    print(
        json.dumps(
            {
                "status": "ok",
                "id": file.get("id"),
                "name": file.get("name"),
                "web_view_link": file.get("webViewLink"),
                "web_content_link": file.get("webContentLink"),
                "parents": file.get("parents", []),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
