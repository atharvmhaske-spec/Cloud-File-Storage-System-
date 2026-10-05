import os
import sqlite3
import boto3
import uuid
import threading

from functools import wraps
from datetime import datetime, timedelta

import boto3

from botocore.config import Config
from botocore.exceptions import ClientError

from boto3.s3.transfer import TransferConfig

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    jsonify
)

from werkzeug.security import (
    generate_password_hash,
    check_password_hash
)

from werkzeug.utils import secure_filename

from dotenv import load_dotenv


# =========================================================
# LOAD ENVIRONMENT VARIABLES
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

ENV_FILE = os.path.join(
    BASE_DIR,
    ".env"
)

load_dotenv(
    ENV_FILE,
    override=True
)


# =========================================================
# FLASK CONFIGURATION
# =========================================================

app = Flask(__name__)

app.secret_key = os.getenv(
    "FLASK_SECRET_KEY",
    "change-this-secret"
)


# =========================================================
# AWS CONFIGURATION
# =========================================================

AWS_REGION = os.getenv(
    "AWS_REGION",
    "ap-south-1"
)

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "atharv-cloud-file-storage-2026"
)

AWS_ACCESS_KEY_ID = os.getenv(
    "AWS_ACCESS_KEY_ID"
)

AWS_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY"
)


# =========================================================
# AWS S3 CLIENT
# =========================================================

s3_config = Config(
    signature_version="s3v4",
    region_name=AWS_REGION,
    s3={
        "addressing_style": "virtual"
    }
)

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=s3_config
)


# =========================================================
# S3 TRANSFER CONFIGURATION
# =========================================================

S3_CHUNK_SIZE = 5 * 1024 * 1024

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=S3_CHUNK_SIZE,
    multipart_chunksize=S3_CHUNK_SIZE,
    max_concurrency=1,
    use_threads=True
)


# =========================================================
# S3 UPLOAD PROGRESS
# =========================================================

upload_progress = {}

upload_progress_lock = threading.Lock()


# =========================================================
# PROGRESS HELPER
# =========================================================

def create_upload_progress(
    upload_id,
    filename,
    total_size
):

    with upload_progress_lock:

        upload_progress[upload_id] = {

            "filename": filename,

            "total": total_size,

            "uploaded": 0,

            "percent": 0,

            "status": "starting",

            "message": "Preparing upload..."

        }


# =========================================================
# UPDATE PROGRESS
# =========================================================

def update_upload_progress(
    upload_id,
    uploaded,
    total_size
):

    if total_size <= 0:

        percent = 100

    else:

        percent = int(
            (
                uploaded
                /
                total_size
            ) * 100
        )

        percent = max(
            0,
            min(
                percent,
                100
            )
        )

    with upload_progress_lock:

        if upload_id in upload_progress:

            upload_progress[upload_id].update({

                "uploaded": uploaded,

                "total": total_size,

                "percent": percent,

                "status": "uploading",

                "message":
                    "Saving data to Amazon S3..."

            })


# =========================================================
# S3 PROGRESS CALLBACK
# =========================================================

class S3ProgressCallback:

    def __init__(
        self,
        upload_id,
        total_size
    ):

        self.upload_id = upload_id

        self.total_size = total_size

        self.lock = threading.Lock()

        self.bytes_transferred = 0

    def __call__(
        self,
        bytes_amount
    ):

        with self.lock:

            self.bytes_transferred += (
                bytes_amount
            )

            update_upload_progress(
                self.upload_id,
                self.bytes_transferred,
                self.total_size
            )


# =========================================================
# MARK UPLOAD COMPLETE
# =========================================================

def mark_upload_complete(
    upload_id
):

    with upload_progress_lock:

        if upload_id in upload_progress:

            total = upload_progress[
                upload_id
            ].get(
                "total",
                0
            )

            upload_progress[
                upload_id
            ].update({

                "uploaded": total,

                "percent": 100,

                "status": "completed",

                "message":
                    "File successfully stored in Amazon S3."

            })


# =========================================================
# MARK UPLOAD FAILED
# =========================================================

def mark_upload_failed(
    upload_id,
    message
):

    with upload_progress_lock:

        if upload_id in upload_progress:

            upload_progress[
                upload_id
            ].update({

                "status": "failed",

                "message": message

            })


# =========================================================
# DATABASE
# =========================================================

DATABASE = os.path.join(
    BASE_DIR,
    "cloudstorage.db"
)


def get_db():

    connection = sqlite3.connect(
        DATABASE
    )

    connection.row_factory = sqlite3.Row

    return connection


# =========================================================
# INITIALIZE DATABASE
# =========================================================

def init_database():

    connection = get_db()

    connection.execute("""
        CREATE TABLE IF NOT EXISTS users (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            name TEXT NOT NULL,

            email TEXT UNIQUE NOT NULL,

            password TEXT NOT NULL

        )
    """)

    connection.execute("""
        CREATE TABLE IF NOT EXISTS files (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            user_id INTEGER NOT NULL,

            name TEXT NOT NULL,

            s3_key TEXT NOT NULL UNIQUE,

            folder TEXT NOT NULL,

            size INTEGER DEFAULT 0,

            file_type TEXT DEFAULT 'file',

            created_at TEXT NOT NULL,

            last_accessed TEXT,

            starred INTEGER DEFAULT 0,

            deleted INTEGER DEFAULT 0,

            shared_with TEXT,

            share_token TEXT UNIQUE,

            share_expires_at TEXT,

            FOREIGN KEY (user_id)
                REFERENCES users(id)

        )
    """)

    # -----------------------------------------------------
    # DATABASE MIGRATION FOR EXISTING INSTALLATIONS
    # -----------------------------------------------------
    columns = [
        row["name"]
        for row in connection.execute(
            "PRAGMA table_info(files)"
        ).fetchall()
    ]

    if "share_token" not in columns:
        connection.execute(
            "ALTER TABLE files ADD COLUMN share_token TEXT"
        )

    if "share_expires_at" not in columns:
        connection.execute(
            "ALTER TABLE files ADD COLUMN share_expires_at TEXT"
        )

    connection.commit()

    connection.close()


# =========================================================
# LOGIN REQUIRED
# =========================================================

def login_required(function):

    @wraps(function)
    def wrapper(*args, **kwargs):

        if "user_id" not in session:

            return redirect(
                url_for("login")
            )

        return function(
            *args,
            **kwargs
        )

    return wrapper


# =========================================================
# USER ROOT FOLDER
# =========================================================

def user_folder():

    return (
        f"users/"
        f"{session['user_id']}/"
    )


# =========================================================
# NORMALIZE FOLDER
# =========================================================

def normalize_folder(folder):

    root = user_folder()

    if not folder:

        return root

    folder = str(folder)

    folder = folder.replace(
        "\\",
        "/"
    )

    folder = folder.strip()

    folder = folder.lstrip("/")

    parts = []

    for part in folder.split("/"):

        if not part:

            continue

        if part in [".", ".."]:

            continue

        parts.append(part)

    clean_folder = "/".join(parts)

    if not clean_folder.startswith(
        root.rstrip("/")
    ):

        return root

    if not clean_folder.endswith("/"):

        clean_folder += "/"

    return clean_folder


# =========================================================
# GET SELECTED FILE KEYS
# =========================================================

def get_selected_file_keys():

    """
    Supports:

    HTML form:
        keys=value1
        keys=value2

    JSON:
        {
            "keys": [
                "value1",
                "value2"
            ]
        }

    Single key:
        keys=value1
    """

    keys = []

    if request.is_json:

        data = request.get_json(
            silent=True
        ) or {}

        json_keys = data.get(
            "keys",
            []
        )

        if isinstance(
            json_keys,
            str
        ):

            json_keys = [
                json_keys
            ]

        if isinstance(
            json_keys,
            list
        ):

            keys = json_keys

    else:

        keys = request.form.getlist(
            "keys"
        )

        if not keys:

            single_key = request.form.get(
                "keys",
                ""
            )

            if single_key:

                keys = [
                    single_key
                ]

    cleaned = []

    seen = set()

    for key in keys:

        key = str(
            key or ""
        ).strip()

        if not key:

            continue

        if key in seen:

            continue

        seen.add(key)

        cleaned.append(
            key
        )

    return cleaned


# =========================================================
# CHECK AJAX REQUEST
# =========================================================

def is_ajax_request():

    return (
        request.headers.get(
            "X-Requested-With"
        )
        ==
        "XMLHttpRequest"
    )


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    return render_template(
        "index.html"
    )


# =========================================================
# SIGN UP
# =========================================================

@app.route(
    "/signup",
    methods=["GET", "POST"]
)
def signup():

    if request.method == "POST":

        name = request.form.get(
            "name",
            ""
        ).strip()

        email = request.form.get(
            "email",
            ""
        ).strip().lower()

        password = request.form.get(
            "password",
            ""
        )

        if not name or not email or not password:

            flash(
                "All fields are required.",
                "error"
            )

            return redirect(
                url_for("signup")
            )

        if len(password) < 6:

            flash(
                "Password must contain at least 6 characters.",
                "error"
            )

            return redirect(
                url_for("signup")
            )

        password_hash = generate_password_hash(
            password
        )

        connection = get_db()

        try:

            cursor = connection.execute(
                """
                INSERT INTO users
                (
                    name,
                    email,
                    password
                )
                VALUES (?, ?, ?)
                """,
                (
                    name,
                    email,
                    password_hash
                )
            )

            user_id = cursor.lastrowid

            connection.commit()

        except sqlite3.IntegrityError:

            connection.close()

            flash(
                "Email already registered.",
                "error"
            )

            return redirect(
                url_for("signup")
            )

        connection.close()

        try:

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"users/{user_id}/",
                Body=b"",
                ContentType="application/x-directory"
            )

        except Exception as error:

            print(
                "S3 USER FOLDER ERROR:",
                error
            )

        flash(
            "Account created successfully. Please login.",
            "success"
        )

        return redirect(
            url_for("login")
        )

    return render_template(
        "signup.html"
    )


# =========================================================
# LOGIN
# =========================================================

@app.route(
    "/login",
    methods=["GET", "POST"]
)
def login():

    if request.method == "POST":

        email = request.form.get(
            "email",
            ""
        ).strip().lower()

        password = request.form.get(
            "password",
            ""
        )

        connection = get_db()

        user = connection.execute(
            """
            SELECT *
            FROM users
            WHERE email = ?
            """,
            (email,)
        ).fetchone()

        connection.close()

        if user:

            valid_password = check_password_hash(
                user["password"],
                password
            )

            if valid_password:

                session.clear()

                session["user_id"] = user["id"]

                session["user_name"] = user["name"]

                session["user_email"] = user["email"]

                return redirect(
                    url_for("dashboard")
                )

        flash(
            "Invalid email or password.",
            "error"
        )

    return render_template(
        "login.html"
    )


# =========================================================
# LOGOUT
# =========================================================

@app.route("/logout")
def logout():

    session.clear()

    flash(
        "You have been logged out.",
        "success"
    )

    return redirect(
        url_for("home")
    )


# =========================================================
# SHARED WITH ME
# =========================================================

@app.route("/shared-with-me")
@login_required
def shared_with_me():

    connection = get_db()

    files = connection.execute(
        """
        SELECT
            files.*,
            users.name AS owner_name,
            users.email AS owner_email

        FROM files

        JOIN users
        ON files.user_id = users.id

        WHERE files.shared_with = ?

        AND files.deleted = 0

        AND files.file_type != 'folder'

        ORDER BY
            datetime(files.created_at) DESC
        """,
        (
            session["user_email"],
        )
    ).fetchall()

    connection.close()

    formatted_files = []

    for row in files:

        file_data = dict(row)

        file_data["key"] = row["s3_key"]

        file_data["type"] = (
            row["file_type"].upper()
            if row["file_type"]
            else "FILE"
        )

        formatted_files.append(
            file_data
        )

    return render_template(
        "shared_with_me.html",
        files=formatted_files
    )


# =========================================================
# RECENT FILES
# =========================================================

@app.route("/recent")
@login_required
def recent_files():

    connection = get_db()

    files = connection.execute(
        """
        SELECT *

        FROM files

        WHERE user_id = ?

        AND deleted = 0

        AND file_type != 'folder'

        ORDER BY
            datetime(
                COALESCE(
                    last_accessed,
                    created_at
                )
            ) DESC

        LIMIT 20
        """,
        (
            session["user_id"],
        )
    ).fetchall()

    connection.close()

    formatted_files = []

    for row in files:

        file_data = dict(row)

        file_data["key"] = row["s3_key"]

        file_data["type"] = (
            row["file_type"].upper()
            if row["file_type"]
            else "FILE"
        )

        formatted_files.append(
            file_data
        )

    return render_template(
        "recent.html",
        files=formatted_files
    )


# =========================================================
# STARRED FILES
# =========================================================

@app.route("/starred")
@login_required
def starred_files():

    connection = get_db()

    files = connection.execute(
        """
        SELECT *

        FROM files

        WHERE user_id = ?

        AND starred = 1

        AND deleted = 0

        AND file_type != 'folder'

        ORDER BY
            datetime(created_at) DESC
        """,
        (
            session["user_id"],
        )
    ).fetchall()

    connection.close()

    formatted_files = []

    for row in files:

        file_data = dict(row)

        file_data["key"] = row["s3_key"]

        file_data["type"] = (
            row["file_type"].upper()
            if row["file_type"]
            else "FILE"
        )

        file_data["starred"] = True

        formatted_files.append(
            file_data
        )

    return render_template(
        "starred.html",
        files=formatted_files
    )


# =========================================================
# STAR FILE
# =========================================================

@app.route(
    "/star/<int:file_id>"
)
@login_required
def star_file(file_id):

    connection = get_db()

    file_record = connection.execute(
        """
        SELECT *

        FROM files

        WHERE id = ?

        AND user_id = ?

        AND deleted = 0

        AND file_type != 'folder'
        """,
        (
            file_id,
            session["user_id"]
        )
    ).fetchone()

    if not file_record:

        connection.close()

        if is_ajax_request():

            return jsonify({
                "success": False,
                "message": "File not found."
            }), 404

        flash(
            "File not found.",
            "error"
        )

        return redirect(
            request.referrer
            or
            url_for("dashboard")
        )

    connection.execute(
        """
        UPDATE files

        SET starred = 1

        WHERE id = ?

        AND user_id = ?
        """,
        (
            file_id,
            session["user_id"]
        )
    )

    connection.commit()

    connection.close()

    if is_ajax_request():

        return jsonify({
            "success": True,
            "message": "File added to Starred."
        })

    flash(
        "File added to Starred.",
        "success"
    )

    return redirect(
        request.referrer
        or
        url_for("dashboard")
    )


# =========================================================
# REMOVE STAR
# =========================================================

@app.route(
    "/unstar/<int:file_id>"
)
@login_required
def unstar_file(file_id):

    connection = get_db()

    cursor = connection.execute(
        """
        UPDATE files

        SET starred = 0

        WHERE id = ?

        AND user_id = ?
        """,
        (
            file_id,
            session["user_id"]
        )
    )

    connection.commit()

    changed = cursor.rowcount > 0

    connection.close()

    if is_ajax_request():

        return jsonify({
            "success": changed,
            "message":
                "File removed from Starred."
                if changed
                else
                "File not found."
        }), (
            200
            if changed
            else 404
        )

    flash(
        "File removed from Starred.",
        "success"
    )

    return redirect(
        request.referrer
        or
        url_for("starred_files")
    )


# =========================================================
# TRASH
# =========================================================

@app.route("/trash")
@login_required
def trash():

    connection = get_db()

    files = connection.execute(
        """
        SELECT *

        FROM files

        WHERE user_id = ?

        AND deleted = 1

        ORDER BY
            datetime(created_at) DESC
        """,
        (
            session["user_id"],
        )
    ).fetchall()

    connection.close()

    formatted_files = []

    for row in files:

        file_data = dict(row)

        file_data["key"] = row["s3_key"]

        file_data["type"] = (
            row["file_type"].upper()
            if row["file_type"]
            else "FILE"
        )

        formatted_files.append(
            file_data
        )

    return render_template(
        "trash.html",
        files=formatted_files
    )


# =========================================================
# MOVE TO TRASH
# =========================================================

@app.route(
    "/trash/<int:file_id>",
    methods=["POST"]
)
@login_required
def move_to_trash(file_id):

    connection = get_db()

    file = connection.execute(
        """
        SELECT *

        FROM files

        WHERE id = ?

        AND user_id = ?

        AND deleted = 0
        """,
        (
            file_id,
            session["user_id"]
        )
    ).fetchone()

    if not file:

        connection.close()

        flash(
            "File not found.",
            "error"
        )

        return redirect(
            request.referrer
            or
            url_for("dashboard")
        )

    if file["file_type"] == "folder":

        folder_key = file["s3_key"]

        connection.execute(
            """
            UPDATE files

            SET deleted = 1

            WHERE user_id = ?

            AND s3_key LIKE ?

            AND deleted = 0
            """,
            (
                session["user_id"],
                folder_key + "%"
            )
        )

        connection.commit()

        connection.close()

        flash(
            "Folder moved to Trash.",
            "success"
        )

        return redirect(
            request.referrer
            or
            url_for("dashboard")
        )

    connection.execute(
        """
        UPDATE files

        SET deleted = 1

        WHERE id = ?

        AND user_id = ?
        """,
        (
            file_id,
            session["user_id"]
        )
    )

    connection.commit()

    connection.close()

    flash(
        "File moved to Trash.",
        "success"
    )

    return redirect(
        request.referrer
        or
        url_for("dashboard")
    )


# =========================================================
# OLD DELETE URL
# =========================================================

@app.route(
    "/delete/<int:file_id>"
)
@login_required
def delete(file_id):

    return move_file_to_trash_by_id(
        file_id
    )


def move_file_to_trash_by_id(file_id):

    connection = get_db()

    file = connection.execute(
        """
        SELECT *

        FROM files

        WHERE id = ?

        AND user_id = ?

        AND deleted = 0
        """,
        (
            file_id,
            session["user_id"]
        )
    ).fetchone()

    if not file:

        connection.close()

        return "File not found", 404

    if file["file_type"] == "folder":

        folder_key = file["s3_key"]

        connection.execute(
            """
            UPDATE files

            SET deleted = 1

            WHERE user_id = ?

            AND s3_key LIKE ?

            AND deleted = 0
            """,
            (
                session["user_id"],
                folder_key + "%"
            )
        )

        connection.commit()

        connection.close()

        flash(
            "Folder moved to Trash.",
            "success"
        )

        return redirect(
            request.referrer
            or
            url_for("dashboard")
        )

    connection.execute(
        """
        UPDATE files

        SET deleted = 1

        WHERE id = ?

        AND user_id = ?
        """,
        (
            file_id,
            session["user_id"]
        )
    )

    connection.commit()

    connection.close()

    flash(
        "File moved to Trash.",
        "success"
    )

    return redirect(
        request.referrer
        or
        url_for("dashboard")
    )


# =========================================================
# RESTORE
# =========================================================

@app.route(
    "/restore/<int:file_id>"
)
@login_required
def restore_file(file_id):

    connection = get_db()

    file = connection.execute(
        """
        SELECT *

        FROM files

        WHERE id = ?

        AND user_id = ?

        AND deleted = 1
        """,
        (
            file_id,
            session["user_id"]
        )
    ).fetchone()

    if not file:

        connection.close()

        flash(
            "File or folder not found in Trash.",
            "error"
        )

        return redirect(
            url_for("trash")
        )

    if file["file_type"] == "folder":

        folder_key = file["s3_key"]

        connection.execute(
            """
            UPDATE files

            SET deleted = 0

            WHERE user_id = ?

            AND s3_key LIKE ?
            """,
            (
                session["user_id"],
                folder_key + "%"
            )
        )

        connection.commit()

        connection.close()

        flash(
            "Folder and its contents restored successfully.",
            "success"
        )

        return redirect(
            url_for("trash")
        )

    connection.execute(
        """
        UPDATE files

        SET deleted = 0

        WHERE id = ?

        AND user_id = ?
        """,
        (
            file_id,
            session["user_id"]
        )
    )

    connection.commit()

    connection.close()

    flash(
        "File restored successfully.",
        "success"
    )

    return redirect(
        url_for("trash")
    )


# =========================================================
# PERMANENT DELETE
# =========================================================

@app.route(
    "/permanent-delete/<int:file_id>"
)
@login_required
def permanent_delete(file_id):

    connection = get_db()

    file = connection.execute(
        """
        SELECT *

        FROM files

        WHERE id = ?

        AND user_id = ?

        AND deleted = 1
        """,
        (
            file_id,
            session["user_id"]
        )
    ).fetchone()

    if not file:

        connection.close()

        return "File or folder not found", 404

    if file["file_type"] == "folder":

        folder_key = file["s3_key"]

        try:

            objects = list_all_objects(
                folder_key
            )

            for start in range(
                0,
                len(objects),
                1000
            ):

                batch = objects[
                    start:start + 1000
                ]

                if not batch:

                    continue

                delete_list = [

                    {
                        "Key": item["Key"]
                    }

                    for item in batch
                ]

                s3.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete={
                        "Objects": delete_list,
                        "Quiet": True
                    }
                )

            connection.execute(
                """
                DELETE FROM files

                WHERE user_id = ?

                AND s3_key LIKE ?
                """,
                (
                    session["user_id"],
                    folder_key + "%"
                )
            )

            connection.commit()

            connection.close()

            flash(
                "Folder permanently deleted.",
                "success"
            )

            return redirect(
                url_for("trash")
            )

        except ClientError as error:

            print(
                "PERMANENT FOLDER DELETE ERROR:",
                error
            )

            connection.close()

            flash(
                "Unable to permanently delete folder: "
                + str(error),
                "error"
            )

            return redirect(
                url_for("trash")
            )

    try:

        s3.delete_object(
            Bucket=S3_BUCKET,
            Key=file["s3_key"]
        )

    except ClientError as error:

        print(
            "S3 DELETE ERROR:",
            error
        )

    connection.execute(
        """
        DELETE FROM files

        WHERE id = ?

        AND user_id = ?
        """,
        (
            file_id,
            session["user_id"]
        )
    )

    connection.commit()

    connection.close()

    flash(
        "File permanently deleted.",
        "success"
    )

    return redirect(
        url_for("trash")
    )


# =========================================================
# LIST ALL S3 OBJECTS
# =========================================================

def list_all_objects(prefix):

    all_objects = []

    continuation_token = None

    while True:

        params = {

            "Bucket": S3_BUCKET,

            "Prefix": prefix

        }

        if continuation_token:

            params[
                "ContinuationToken"
            ] = continuation_token

        response = s3.list_objects_v2(
            **params
        )

        all_objects.extend(
            response.get(
                "Contents",
                []
            )
        )

        if response.get(
            "IsTruncated"
        ):

            continuation_token = response.get(
                "NextContinuationToken"
            )

        else:

            break

    return all_objects


# =========================================================
# GET CURRENT FOLDER CONTENTS
# =========================================================

def get_folder_contents(current_prefix):

    files = []

    folders = []

    objects = list_all_objects(
        current_prefix
    )

    folder_names = set()

    for item in objects:

        key = item["Key"]

        relative = key[
            len(current_prefix):
        ]

        if not relative:

            continue

        if "/" in relative:

            first_folder = relative.split(
                "/",
                1
            )[0]

            if first_folder:

                folder_names.add(
                    first_folder
                )

            continue

        if key.endswith("/"):

            folder_name = relative.rstrip("/")

            if folder_name:

                folder_names.add(
                    folder_name
                )

            continue

        filename = key.split("/")[-1]

        files.append({

            "key": key,

            "name": filename,

            "size": item["Size"],

            "modified":
                item["LastModified"].strftime(
                    "%d %b %Y %H:%M"
                ),

            "type":
                filename.rsplit(
                    ".",
                    1
                )[1].upper()
                if "." in filename
                else "FILE"

        })

    for folder_name in sorted(
        folder_names,
        key=str.lower
    ):

        folder_key = (
            current_prefix
            + folder_name
            + "/"
        )

        folders.append({

            "name": folder_name,

            "key": folder_key

        })

    files.sort(
        key=lambda x:
            x["name"].lower()
    )

    return files, folders


# =========================================================
# DASHBOARD
# =========================================================

@app.route("/dashboard")
@login_required
def dashboard():

    requested_folder = request.args.get(
        "folder",
        ""
    )

    current_folder = normalize_folder(
        requested_folder
    )

    root = user_folder()

    files = []

    folders = []

    storage_used = 0

    storage_limit = (
        5 * 1024 * 1024 * 1024
    )

    storage_percentage = 0

    if not S3_BUCKET:

        flash(
            "S3_BUCKET is not configured in .env file.",
            "error"
        )

        return render_template(
            "dashboard.html",

            files=files,

            folders=folders,

            current_folder=current_folder,

            breadcrumbs=[],

            is_root=True,

            storage_used=storage_used,

            storage_limit=storage_limit,

            storage_percentage=storage_percentage
        )

    try:

        files, folders = get_folder_contents(
            current_folder
        )

        connection = get_db()

        database_files = connection.execute(
            """
            SELECT
                id,
                name,
                s3_key,
                file_type,
                starred,
                deleted,
                created_at,
                last_accessed,
                shared_with

            FROM files

            WHERE user_id = ?
            """,
            (
                session["user_id"],
            )
        ).fetchall()

        storage_result = connection.execute(
            """
            SELECT
                COALESCE(
                    SUM(size),
                    0
                ) AS total_size

            FROM files

            WHERE user_id = ?

            AND deleted = 0

            AND file_type != 'folder'
            """,
            (
                session["user_id"],
            )
        ).fetchone()

        storage_used = (
            storage_result["total_size"]
            or 0
        )

        storage_percentage = min(
            (
                storage_used
                /
                storage_limit
            ) * 100,
            100
        )

        connection.close()

        file_metadata = {

            row["s3_key"]: row

            for row in database_files

        }

        visible_files = []

        for file in files:

            key = file["key"]

            database_file = file_metadata.get(
                key
            )

            if database_file:

                if database_file["deleted"] == 1:

                    continue

                file["id"] = (
                    database_file["id"]
                )

                file["name"] = (
                    database_file["name"]
                )

                file["starred"] = bool(
                    database_file["starred"]
                )

                file["deleted"] = bool(
                    database_file["deleted"]
                )

                file["created_at"] = (
                    database_file["created_at"]
                )

                file["last_accessed"] = (
                    database_file["last_accessed"]
                )

                file["shared_with"] = (
                    database_file["shared_with"]
                )

            else:

                file["id"] = None

                file["starred"] = False

                file["deleted"] = False

                file["created_at"] = None

                file["last_accessed"] = None

                file["shared_with"] = None

            visible_files.append(
                file
            )

        files = visible_files

        visible_folders = []

        for folder in folders:

            folder_key = folder["key"]

            database_folder = file_metadata.get(
                folder_key
            )

            if database_folder:

                if database_folder["deleted"] == 1:

                    continue

                folder["id"] = (
                    database_folder["id"]
                )

                folder["deleted"] = False

            else:

                folder["id"] = None

                folder["deleted"] = False

            visible_folders.append(
                folder
            )

        folders = visible_folders

    except ClientError as error:

        print(
            "S3 DASHBOARD ERROR:",
            error
        )

        flash(
            "AWS S3 Error: "
            + str(error),
            "error"
        )

    except Exception as error:

        print(
            "DASHBOARD ERROR:",
            error
        )

        flash(
            "Dashboard Error: "
            + str(error),
            "error"
        )

    breadcrumbs = []

    relative_path = current_folder[
        len(root):
    ].strip("/")

    if relative_path:

        accumulated = root

        for part in relative_path.split("/"):

            accumulated += (
                part
                + "/"
            )

            breadcrumbs.append({

                "name": part,

                "key": accumulated

            })

    return render_template(

        "dashboard.html",

        files=files,

        folders=folders,

        current_folder=current_folder,

        breadcrumbs=breadcrumbs,

        is_root=(
            current_folder == root
        ),

        storage_used=storage_used,

        storage_limit=storage_limit,

        storage_percentage=storage_percentage

    )


# =========================================================
# CREATE FOLDER
# =========================================================

@app.route(
    "/create-folder",
    methods=["POST"]
)
@login_required
def create_folder():

    folder_name = request.form.get(
        "folder_name",
        ""
    ).strip()

    parent_folder = normalize_folder(
        request.form.get(
            "parent_folder",
            ""
        )
    )

    if not S3_BUCKET:

        flash(
            "S3_BUCKET is not configured.",
            "error"
        )

        return redirect(
            url_for(
                "dashboard",
                folder=parent_folder
            )
        )

    if not folder_name:

        flash(
            "Please enter a folder name.",
            "error"
        )

        return redirect(
            url_for(
                "dashboard",
                folder=parent_folder
            )
        )

    folder_name = secure_filename(
        folder_name
    )

    if not folder_name:

        flash(
            "Invalid folder name.",
            "error"
        )

        return redirect(
            url_for(
                "dashboard",
                folder=parent_folder
            )
        )

    folder_key = (
        parent_folder
        + folder_name
        + "/"
    )

    connection = get_db()

    existing_folder = connection.execute(
        """
        SELECT *

        FROM files

        WHERE user_id = ?

        AND s3_key = ?

        AND file_type = 'folder'
        """,
        (
            session["user_id"],
            folder_key
        )
    ).fetchone()

    if existing_folder:

        if existing_folder["deleted"] == 1:

            connection.execute(
                """
                UPDATE files

                SET deleted = 0

                WHERE id = ?

                AND user_id = ?
                """,
                (
                    existing_folder["id"],
                    session["user_id"]
                )
            )

            connection.commit()

            connection.close()

            try:

                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=folder_key,
                    Body=b"",
                    ContentType="application/x-directory"
                )

            except Exception as error:

                print(
                    "S3 FOLDER RESTORE ERROR:",
                    error
                )

            flash(
                f"Folder '{folder_name}' restored.",
                "success"
            )

            return redirect(
                url_for(
                    "dashboard",
                    folder=parent_folder
                )
            )

        connection.close()

        flash(
            f"Folder '{folder_name}' already exists.",
            "error"
        )

        return redirect(
            url_for(
                "dashboard",
                folder=parent_folder
            )
        )

    try:

        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=folder_key,
            MaxKeys=1
        )

        if response.get(
            "KeyCount",
            0
        ) > 0:

            connection.close()

            flash(
                f"Folder '{folder_name}' already exists.",
                "error"
            )

            return redirect(
                url_for(
                    "dashboard",
                    folder=parent_folder
                )
            )

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=folder_key,
            Body=b"",
            ContentType="application/x-directory"
        )

        now = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        connection.execute(
            """
            INSERT INTO files
            (
                user_id,
                name,
                s3_key,
                folder,
                size,
                file_type,
                created_at,
                last_accessed,
                starred,
                deleted,
                shared_with
            )

            VALUES
            (
                ?,
                ?,
                ?,
                ?,
                0,
                'folder',
                ?,
                ?,
                0,
                0,
                NULL
            )
            """,
            (
                session["user_id"],
                folder_name,
                folder_key,
                parent_folder,
                now,
                now
            )
        )

        connection.commit()

        connection.close()

        flash(
            f"Folder '{folder_name}' created successfully.",
            "success"
        )

    except sqlite3.IntegrityError as error:

        connection.rollback()

        connection.close()

        print(
            "CREATE FOLDER DATABASE ERROR:",
            error
        )

        try:

            s3.delete_object(
                Bucket=S3_BUCKET,
                Key=folder_key
            )

        except Exception:

            pass

        flash(
            "Folder could not be created.",
            "error"
        )

    except ClientError as error:

        connection.close()

        print(
            "CREATE FOLDER ERROR:",
            error
        )

        flash(
            "Unable to create folder: "
            + str(error),
            "error"
        )

    return redirect(
        url_for(
            "dashboard",
            folder=parent_folder
        )
    )


# =========================================================
# OPEN FOLDER
# =========================================================

@app.route("/open-folder")
@login_required
def open_folder():

    folder = normalize_folder(
        request.args.get(
            "folder",
            ""
        )
    )

    if not folder.startswith(
        user_folder()
    ):

        return "Access Denied", 403

    root = user_folder()

    if folder != root:

        connection = get_db()

        folder_record = connection.execute(
            """
            SELECT *

            FROM files

            WHERE user_id = ?

            AND s3_key = ?

            AND file_type = 'folder'
            """,
            (
                session["user_id"],
                folder
            )
        ).fetchone()

        connection.close()

        if folder_record and folder_record["deleted"] == 1:

            flash(
                "This folder is in Trash.",
                "error"
            )

            return redirect(
                url_for("dashboard")
            )

    return redirect(
        url_for(
            "dashboard",
            folder=folder
        )
    )


# =========================================================
# UPLOAD PROGRESS API
# =========================================================

@app.route(
    "/upload-progress/<upload_id>"
)
@login_required
def upload_progress_api(upload_id):

    with upload_progress_lock:

        progress = upload_progress.get(
            upload_id
        )

        if not progress:

            return jsonify({

                "success": False,

                "message":
                    "Upload progress not found."

            }), 404

        return jsonify({

            "success": True,

            "upload_id": upload_id,

            "filename":
                progress["filename"],

            "uploaded":
                progress["uploaded"],

            "total":
                progress["total"],

            "percent":
                progress["percent"],

            "status":
                progress["status"],

            "message":
                progress["message"]

        })


# =========================================================
# CLEAN COMPLETED PROGRESS
# =========================================================

@app.route(
    "/upload-progress/<upload_id>",
    methods=["DELETE"]
)
@login_required
def delete_upload_progress(upload_id):

    with upload_progress_lock:

        upload_progress.pop(
            upload_id,
            None
        )

    return jsonify({

        "success": True

    })


# =========================================================
# UPLOAD
# =========================================================

@app.route(
    "/upload",
    methods=["GET", "POST"]
)
@login_required
def upload():

    if request.method == "GET":

        folder = normalize_folder(
            request.args.get(
                "folder",
                ""
            )
        )

        return render_template(
            "upload.html",
            current_folder=folder
        )

    is_ajax = is_ajax_request()

    folder = normalize_folder(
        request.form.get(
            "folder",
            ""
        )
    )

    if not folder.startswith(
        user_folder()
    ):

        if is_ajax:

            return jsonify({

                "success": False,

                "message":
                    "Access Denied"

            }), 403

        return "Access Denied", 403

    if not S3_BUCKET:

        message = (
            "S3_BUCKET is not configured in .env file."
        )

        if is_ajax:

            return jsonify({

                "success": False,

                "message": message

            }), 500

        flash(
            message,
            "error"
        )

        return redirect(
            url_for(
                "dashboard",
                folder=folder
            )
        )

    if folder != user_folder():

        connection = get_db()

        deleted_folder = connection.execute(
            """
            SELECT *

            FROM files

            WHERE user_id = ?

            AND s3_key = ?

            AND file_type = 'folder'

            AND deleted = 1
            """,
            (
                session["user_id"],
                folder
            )
        ).fetchone()

        connection.close()

        if deleted_folder:

            message = (
                "Cannot upload into a folder in Trash."
            )

            if is_ajax:

                return jsonify({

                    "success": False,

                    "message": message

                }), 400

            flash(
                message,
                "error"
            )

            return redirect(
                url_for("dashboard")
            )

    uploaded_files = request.files.getlist(
        "files"
    )

    if not uploaded_files:

        single_file = request.files.get(
            "file"
        )

        if single_file:

            uploaded_files = [
                single_file
            ]

    if not uploaded_files:

        message = (
            "Please select at least one file."
        )

        if is_ajax:

            return jsonify({

                "success": False,

                "message": message

            }), 400

        flash(
            message,
            "error"
        )

        return redirect(
            url_for(
                "upload",
                folder=folder
            )
        )

    upload_id = request.form.get(
        "upload_id",
        ""
    ).strip()

    if not upload_id:

        upload_id = str(
            uuid.uuid4()
        )

    uploaded_count = 0

    failed_count = 0

    uploaded_file_names = []

    failed_file_names = []

    connection = get_db()

    for file in uploaded_files:

        if not file:

            continue

        if not file.filename:

            continue

        filename = secure_filename(
            file.filename
        )

        if not filename:

            failed_count += 1

            failed_file_names.append(
                file.filename
                or
                "Unknown file"
            )

            continue

        unique_name = (
            str(uuid.uuid4())
            + "_"
            + filename
        )

        s3_key = (
            folder
            + unique_name
        )

        try:

            file.seek(
                0,
                os.SEEK_END
            )

            file_size = file.tell()

            file.seek(0)

            create_upload_progress(
                upload_id,
                filename,
                file_size
            )

            progress_callback = (
                S3ProgressCallback(
                    upload_id,
                    file_size
                )
            )

            with upload_progress_lock:

                upload_progress[
                    upload_id
                ]["status"] = "uploading"

                upload_progress[
                    upload_id
                ]["message"] = (
                    "Storing data in Amazon S3..."
                )

            s3.upload_fileobj(

                file,

                S3_BUCKET,

                s3_key,

                ExtraArgs={

                    "ContentType":
                        file.content_type
                        or
                        "application/octet-stream"

                },

                Config=TRANSFER_CONFIG,

                Callback=progress_callback
            )

            s3.head_object(
                Bucket=S3_BUCKET,
                Key=s3_key
            )

            mark_upload_complete(
                upload_id
            )

            now = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            file_extension = (

                filename.rsplit(
                    ".",
                    1
                )[1].lower()

                if "." in filename

                else "file"

            )

            connection.execute(
                """
                INSERT INTO files
                (
                    user_id,
                    name,
                    s3_key,
                    folder,
                    size,
                    file_type,
                    created_at,
                    last_accessed,
                    starred,
                    deleted,
                    shared_with
                )

                VALUES
                (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    0,
                    0,
                    NULL
                )
                """,
                (
                    session["user_id"],
                    filename,
                    s3_key,
                    folder,
                    file_size,
                    file_extension,
                    now,
                    now
                )
            )

            uploaded_count += 1

            uploaded_file_names.append(
                filename
            )

        except sqlite3.IntegrityError as error:

            failed_count += 1

            failed_file_names.append(
                filename
            )

            print(
                "UPLOAD DATABASE ERROR:",
                error
            )

            mark_upload_failed(
                upload_id,
                "Database error while saving file."
            )

            try:

                s3.delete_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key
                )

            except Exception:

                pass

        except Exception as error:

            failed_count += 1

            failed_file_names.append(
                filename
            )

            print(
                "UPLOAD ERROR:",
                error
            )

            mark_upload_failed(
                upload_id,
                "S3 upload failed: "
                + str(error)
            )

            try:

                s3.delete_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key
                )

            except Exception:

                pass

    try:

        connection.commit()

    except Exception as error:

        print(
            "UPLOAD DATABASE COMMIT ERROR:",
            error
        )

        connection.rollback()

    finally:

        connection.close()

    if is_ajax:

        if uploaded_count > 0:

            if failed_count > 0:

                message = (
                    f"{uploaded_count} file(s) uploaded "
                    f"successfully, "
                    f"{failed_count} file(s) failed."
                )

            else:

                message = (
                    f"{uploaded_count} file(s) uploaded "
                    f"successfully."
                )

            return jsonify({

                "success": True,

                "uploaded_count":
                    uploaded_count,

                "failed_count":
                    failed_count,

                "uploaded_files":
                    uploaded_file_names,

                "failed_files":
                    failed_file_names,

                "message":
                    message,

                "folder":
                    folder,

                "upload_id":
                    upload_id

            }), 200

        return jsonify({

            "success": False,

            "uploaded_count":
                uploaded_count,

            "failed_count":
                failed_count,

            "uploaded_files":
                uploaded_file_names,

            "failed_files":
                failed_file_names,

            "message":
                "File upload failed.",

            "folder":
                folder,

            "upload_id":
                upload_id

        }), 500

    if uploaded_count > 0:

        flash(
            f"{uploaded_count} file(s) uploaded successfully.",
            "success"
        )

    if failed_count > 0:

        flash(
            f"{failed_count} file(s) failed to upload.",
            "error"
        )

    return redirect(
        url_for(
            "dashboard",
            folder=folder
        )
    )


# =========================================================
# SHARING HELPERS
# =========================================================

def share_is_active(file_record):
    """Return True when the database sharing link is still valid."""

    expires_at = file_record["share_expires_at"]

    # NULL/empty expiry means "No Limit".
    if not expires_at:
        return True

    try:
        expires = datetime.strptime(
            expires_at,
            "%Y-%m-%d %H:%M:%S"
        )
    except (ValueError, TypeError):
        return False

    return datetime.now() <= expires


def user_can_access_file(file_record):
    """Owner access or an active token-based shared permission."""

    if file_record["user_id"] == session.get("user_id"):
        return True

    # Logged-in recipient can still see Shared with me, but access is
    # controlled by the same expiry stored for the generated link.
    if (
        session.get("user_email")
        and file_record["shared_with"] == session.get("user_email")
        and share_is_active(file_record)
    ):
        return True

    return False


def create_share_token():
    """Create a cryptographically strong URL-safe sharing token."""
    return uuid.uuid4().hex + uuid.uuid4().hex


# =========================================================
# PUBLIC SHARE LINK
# =========================================================

@app.route("/shared/<token>")
def public_shared_file(token):

    token = str(token or "").strip()

    if not token or len(token) > 100:
        return "Invalid sharing link.", 400

    connection = get_db()

    file_record = connection.execute(
        """
        SELECT *
        FROM files
        WHERE share_token = ?
        AND deleted = 0
        AND file_type != 'folder'
        """,
        (token,)
    ).fetchone()

    if not file_record:
        connection.close()
        return render_template(
            "shared_preview.html",
            error="This sharing link is invalid or has been removed."
        ), 404

    if not share_is_active(file_record):
        connection.close()
        return render_template(
            "shared_preview.html",
            error="This sharing link has expired."
        ), 410

    key = file_record["s3_key"]
    filename = file_record["name"]

    try:
        metadata = s3.head_object(
            Bucket=S3_BUCKET,
            Key=key
        )

        content_type = metadata.get(
            "ContentType",
            "application/octet-stream"
        )

        # Keep the public application link authoritative. The S3 URL is
        # generated only after the token and expiry have been validated.
        download_requested = request.args.get("download") == "1"

        params = {
            "Bucket": S3_BUCKET,
            "Key": key
        }

        if download_requested:
            params["ResponseContentDisposition"] = (
                f'attachment; filename="{secure_filename(filename)}"'
            )
        else:
            params["ResponseContentDisposition"] = "inline"

        # The CloudStorage share token is the authoritative link.
        # The S3 URL only needs to be temporary and is regenerated whenever
        # the public /shared/<token> page is opened.
        if file_record["share_expires_at"]:
            try:
                expires_at = datetime.strptime(
                    file_record["share_expires_at"],
                    "%Y-%m-%d %H:%M:%S"
                )
                remaining_seconds = int(
                    (expires_at - datetime.now()).total_seconds()
                )
            except (ValueError, TypeError):
                connection.close()
                return render_template(
                    "shared_preview.html",
                    error="This sharing link has an invalid expiry value."
                ), 410

            s3_expiry = max(1, min(900, remaining_seconds))
        else:
            # NULL means No Limit. The application token remains active;
            # this generated S3 URL is simply valid for 15 minutes.
            s3_expiry = 900

        url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=s3_expiry,
            HttpMethod="GET"
        )

        if download_requested:
            return redirect(url)

        extension = (
            filename.rsplit(".", 1)[1].lower()
            if "." in filename
            else ""
        )

        image_extensions = {
            "jpg", "jpeg", "png", "gif", "webp",
            "bmp", "svg", "ico", "avif"
        }

        video_extensions = {
            "mp4", "webm", "ogg", "mov", "avi", "mkv"
        }

        audio_extensions = {
            "mp3", "wav", "ogg", "m4a", "aac", "flac"
        }

        return render_template(
            "shared_preview.html",
            error=None,
            name=filename,
            url=url,
            content_type=content_type,
            extension=extension,
            is_image=extension in image_extensions,
            is_video=extension in video_extensions,
            is_audio=extension in audio_extensions,
            expires_at=file_record["share_expires_at"],
            download_url=url_for(
                "public_shared_file",
                token=token,
                download=1
            )
        )

    except ClientError as error:
        print("PUBLIC SHARE S3 ERROR:", error)
        error_code = str(
            error.response
            .get("Error", {})
            .get("Code", "")
        )
        connection.close()

        if error_code in ("404", "NoSuchKey", "NotFound"):
            return render_template(
                "shared_preview.html",
                error="The shared file no longer exists in Amazon S3."
            ), 404

        return render_template(
            "shared_preview.html",
            error="Unable to open the shared file."
        ), 500
    except Exception as error:
        print("PUBLIC SHARE ERROR:", error)
        connection.close()
        return render_template(
            "shared_preview.html",
            error="Unable to open the shared file."
        ), 500
    finally:
        try:
            connection.close()
        except Exception:
            pass


# =========================================================
# DOWNLOAD
# =========================================================

@app.route(
    "/download/<path:key>"
)
@login_required
def download(key):

    connection = get_db()

    file_record = connection.execute(
        """
        SELECT *

        FROM files

        WHERE s3_key = ?

        AND deleted = 0
        """,
        (key,)
    ).fetchone()

    if not file_record:

        connection.close()

        return "File not found", 404

    allowed = user_can_access_file(
        file_record
    )

    if not allowed:

        connection.close()

        return "Access Denied", 403

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    if file_record["user_id"] == session["user_id"]:

        connection.execute(
            """
            UPDATE files

            SET last_accessed = ?

            WHERE id = ?
            """,
            (
                now,
                file_record["id"]
            )
        )

        connection.commit()

    connection.close()

    try:

        s3.head_object(
            Bucket=S3_BUCKET,
            Key=key
        )

        url = s3.generate_presigned_url(

            ClientMethod="get_object",

            Params={

                "Bucket": S3_BUCKET,

                "Key": key,

                "ResponseContentDisposition":
                    "attachment"

            },

            ExpiresIn=900,

            HttpMethod="GET"

        )

        return redirect(url)

    except ClientError as error:

        error_code = str(
            error.response
            .get("Error", {})
            .get("Code", "")
        )

        print(
            "DOWNLOAD ERROR:",
            error
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):

            return (
                "File no longer exists in Amazon S3.",
                404
            )

        return (
            "Unable to download file.",
            500
        )


# =========================================================
# PREVIEW
# =========================================================

@app.route(
    "/preview/<path:key>"
)
@login_required
def preview(key):

    connection = get_db()

    file_record = connection.execute(
        """
        SELECT *

        FROM files

        WHERE s3_key = ?

        AND deleted = 0
        """,
        (key,)
    ).fetchone()

    if not file_record:

        connection.close()

        return "File not found", 404

    allowed = user_can_access_file(
        file_record
    )

    if not allowed:

        connection.close()

        return "Access Denied", 403

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    if file_record["user_id"] == session["user_id"]:

        connection.execute(
            """
            UPDATE files

            SET last_accessed = ?

            WHERE id = ?
            """,
            (
                now,
                file_record["id"]
            )
        )

        connection.commit()

    connection.close()

    try:

        metadata = s3.head_object(
            Bucket=S3_BUCKET,
            Key=key
        )

        url = s3.generate_presigned_url(

            ClientMethod="get_object",

            Params={

                "Bucket": S3_BUCKET,

                "Key": key

            },

            ExpiresIn=900,

            HttpMethod="GET"

        )

        filename = file_record["name"]

        content_type = metadata.get(
            "ContentType",
            "application/octet-stream"
        )

        return render_template(

            "preview.html",

            name=filename,

            url=url,

            content_type=content_type

        )

    except ClientError as error:

        error_code = str(
            error.response
            .get("Error", {})
            .get("Code", "")
        )

        print(
            "PREVIEW ERROR:",
            error
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):

            return (
                "File no longer exists in Amazon S3.",
                404
            )

        return (
            "Unable to preview file.",
            500
        )


# =========================================================
# IMAGE FILE PREVIEW / THUMBNAIL
# =========================================================

@app.route(
    "/file-preview"
)
@login_required
def file_preview():

    key = request.args.get(
        "key",
        ""
    ).strip()

    if not key:

        return (
            "Missing file key",
            400
        )

    # -----------------------------------------------------
    # IMPORTANT:
    # Do NOT check key.startswith(user_folder()) here.
    #
    # A shared file belongs to the owner's S3 folder.
    # Therefore a shared user will have another user's
    # users/<id>/ prefix.
    #
    # Access is checked using the database below.
    # -----------------------------------------------------

    connection = get_db()

    file_record = connection.execute(
        """
        SELECT *

        FROM files

        WHERE s3_key = ?

        AND file_type != 'folder'
        """,
        (
            key,
        )
    ).fetchone()

    connection.close()

    if not file_record:

        return (
            "File not found",
            404
        )

    allowed = user_can_access_file(
        file_record
    )

    if not allowed:

        return (
            "Access Denied",
            403
        )

    filename = file_record["name"]

    extension = (

        filename.rsplit(
            ".",
            1
        )[1].lower()

        if "." in filename

        else ""

    )

    allowed_extensions = {

        "jpg",
        "jpeg",
        "png",
        "gif",
        "webp",
        "bmp",
        "svg",
        "ico",
        "avif"

    }

    if extension not in allowed_extensions:

        return (
            "Image preview not supported",
            415
        )

    if not S3_BUCKET:

        return (
            "S3 bucket is not configured.",
            503
        )

    try:

        metadata = s3.head_object(

            Bucket=S3_BUCKET,

            Key=key

        )

        url = s3.generate_presigned_url(

            ClientMethod="get_object",

            Params={

                "Bucket": S3_BUCKET,

                "Key": key

            },

            ExpiresIn=300,

            HttpMethod="GET"

        )

        print(

            "FILE PREVIEW SUCCESS:",

            key,

            "| ContentType:",

            metadata.get(
                "ContentType",
                "unknown"
            )

        )

        return redirect(
            url
        )

    except ClientError as error:

        error_code = str(
            error.response
            .get("Error", {})
            .get("Code", "")
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):

            print(
                "FILE PREVIEW S3 OBJECT NOT FOUND:",
                key
            )

            return (
                "Image file no longer exists in Amazon S3.",
                404
            )

        print(
            "FILE PREVIEW AWS ERROR:",
            error
        )

        return (
            "Unable to preview image.",
            500
        )

    except Exception as error:

        print(
            "FILE PREVIEW ERROR:",
            error
        )

        return (
            "Unable to preview image.",
            500
        )


# =========================================================
# DELETE FILE BY S3 KEY
# =========================================================

@app.route(
    "/delete-file/<path:key>",
    methods=["POST"]
)
@login_required
def delete_file(key):

    connection = get_db()

    file_record = connection.execute(
        """
        SELECT *

        FROM files

        WHERE s3_key = ?

        AND deleted = 0
        """,
        (
            key,
        )
    ).fetchone()

    if not file_record:

        connection.close()

        if is_ajax_request():

            return jsonify({

                "success": False,

                "message":
                    "File not found."

            }), 404

        flash(
            "File not found.",
            "error"
        )

        return redirect(
            request.referrer
            or
            url_for("dashboard")
        )

    # -----------------------------------------------------
    # OWNER DELETE
    # -----------------------------------------------------

    if file_record["user_id"] == session["user_id"]:

        connection.execute(
            """
            UPDATE files

            SET deleted = 1

            WHERE id = ?

            AND user_id = ?
            """,
            (
                file_record["id"],
                session["user_id"]
            )
        )

        connection.commit()

        connection.close()

        if is_ajax_request():

            return jsonify({

                "success": True,

                "message":
                    "File moved to Trash."

            })

        flash(
            "File moved to Trash.",
            "success"
        )

        return redirect(
            request.referrer
            or
            url_for("dashboard")
        )

    # -----------------------------------------------------
    # SHARED USER REMOVE ACCESS
    # -----------------------------------------------------

    if file_record["shared_with"] == session["user_email"]:

        connection.execute(
            """
            UPDATE files

            SET shared_with = NULL

            WHERE id = ?

            AND shared_with = ?
            """,
            (
                file_record["id"],
                session["user_email"]
            )
        )

        connection.commit()

        connection.close()

        if is_ajax_request():

            return jsonify({

                "success": True,

                "message":
                    "File removed from Shared with me."

            })

        flash(
            "File removed from Shared with me.",
            "success"
        )

        return redirect(
            url_for("shared_with_me")
        )

    connection.close()

    if is_ajax_request():

        return jsonify({

            "success": False,

            "message":
                "You do not have permission to delete this file."

        }), 403

    flash(
        "You do not have permission to delete this file.",
        "error"
    )

    return redirect(
        request.referrer
        or
        url_for("dashboard")
    )


# =========================================================
# SHARE SINGLE FILE / BULK SHARE
# =========================================================
# Generates DIRECT Amazon S3 presigned URLs.
#
# Example:
# https://your-bucket.s3.ap-south-1.amazonaws.com/users/1/file.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256&...
#
# IMPORTANT:
# - No /shared/<token> route is required.
# - No application-generated sharing token is used.
# - The generated URL is the real temporary S3 URL.
# =========================================================


# =========================================================
# ALLOWED SHARE EXPIRY OPTIONS
# =========================================================

ALLOWED_SHARE_MINUTES = {
    15,
    60,
    1440,
    10080
}


# =========================================================
# SHARE EXPIRY LABEL
# =========================================================

def get_s3_share_expiry_label(minutes):
    """
    Convert expiry minutes into a user-friendly label.
    """

    labels = {
        15: "15 Minutes",
        60: "1 Hour",
        1440: "1 Day",
        10080: "7 Days"
    }

    return labels.get(minutes, "15 Minutes")


# =========================================================
# GENERATE DIRECT S3 PRESIGNED URL
# =========================================================

def generate_direct_s3_share_url(key, minutes):
    """
    Generate a temporary DIRECT Amazon S3 GET URL.

    The returned URL points directly to S3.
    It does not pass through Flask.
    """

    if not S3_BUCKET:
        raise RuntimeError(
            "S3_BUCKET is not configured."
        )

    if not key:
        raise ValueError(
            "S3 object key is required."
        )

    # Confirm that the object really exists.
    s3.head_object(
        Bucket=S3_BUCKET,
        Key=key
    )

    # Generate the direct S3 URL.
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={
            "Bucket": S3_BUCKET,
            "Key": key
        },
        ExpiresIn=minutes * 60,
        HttpMethod="GET"
    )


# =========================================================
# READ SHARE EXPIRY
# =========================================================

def get_requested_share_minutes():
    """
    Read the selected expiry value from either:
    - normal HTML form
    - JSON request

    Returns:
        (minutes, error_message)
    """

    raw_minutes = request.form.get(
        "minutes",
        15
    )

    if request.is_json:
        data = request.get_json(
            silent=True
        ) or {}

        raw_minutes = data.get(
            "minutes",
            15
        )

    try:
        minutes = int(raw_minutes)
    except (
        ValueError,
        TypeError
    ):
        return (
            None,
            "Invalid link expiry option."
        )

    if minutes not in ALLOWED_SHARE_MINUTES:
        return (
            None,
            "Invalid link expiry option."
        )

    return (
        minutes,
        None
    )


# =========================================================
# BUILD FILE DATA FOR SHARE PAGE
# =========================================================

def build_share_file_data(row):
    """
    Convert a database file row into the dictionary expected
    by share.html.
    """

    filename = row["name"]

    extension = ""

    if "." in filename:
        extension = (
            filename.rsplit(
                ".",
                1
            )[1]
            .lower()
        )

    return {
        "id": row["id"],
        "name": filename,
        "key": row["s3_key"],
        "s3_key": row["s3_key"],
        "size": row["size"],
        "file_type": row["file_type"],
        "extension": extension,

        # These fields may be used by your dashboard/share UI.
        "created_at": row["created_at"],
        "last_accessed": row["last_accessed"],
        "starred": bool(row["starred"])
    }


# =========================================================
# SHARE SINGLE FILE
# =========================================================

@app.route(
    "/share/<path:key>",
    methods=["GET", "POST"]
)
@login_required
def share(key):

    connection = get_db()

    try:

        # -------------------------------------------------
        # SECURITY:
        # Only allow the logged-in user to share their own
        # active files.
        # -------------------------------------------------

        file_record = connection.execute(
            """
            SELECT *
            FROM files
            WHERE s3_key = ?
            AND user_id = ?
            AND deleted = 0
            AND file_type != 'folder'
            """,
            (
                key,
                session["user_id"]
            )
        ).fetchone()

        if not file_record:

            return (
                "File not found",
                404
            )

        selected_file = build_share_file_data(
            file_record
        )

        selected_files = [
            selected_file
        ]

        # -------------------------------------------------
        # GET
        # -------------------------------------------------

        if request.method == "GET":

            return render_template(
                "share.html",

                files=selected_files,

                name=selected_file["name"],

                share_url=None,

                share_links=[],

                generated_links=[],

                minutes=15,

                shared_with="",

                failed_files=[]
            )

        # -------------------------------------------------
        # POST
        # -------------------------------------------------

        minutes, expiry_error = (
            get_requested_share_minutes()
        )

        if expiry_error:

            if request.is_json:

                return jsonify({
                    "success": False,
                    "message": expiry_error
                }), 400

            flash(
                expiry_error,
                "error"
            )

            return render_template(
                "share.html",
                files=selected_files,
                name=selected_file["name"],
                share_url=None,
                share_links=[],
                generated_links=[],
                minutes=15,
                shared_with="",
                failed_files=[]
            ), 400

        # -------------------------------------------------
        # GENERATE DIRECT S3 URL
        # -------------------------------------------------

        try:

            share_url = (
                generate_direct_s3_share_url(
                    key,
                    minutes
                )
            )

        except ClientError as error:

            print(
                "SHARE S3 ERROR:",
                error
            )

            error_code = (
                error.response
                .get("Error", {})
                .get("Code", "")
            )

            if request.is_json:

                return jsonify({
                    "success": False,
                    "message": (
                        "The S3 file could not be found "
                        "or AWS access was denied."
                    ),
                    "error": error_code
                }), 404

            flash(
                "Unable to create the Amazon S3 sharing link. "
                "Please check that the file exists and AWS "
                "credentials have permission to access it.",
                "error"
            )

            return render_template(
                "share.html",
                files=selected_files,
                name=selected_file["name"],
                share_url=None,
                share_links=[],
                generated_links=[],
                minutes=minutes,
                shared_with="",
                failed_files=[]
            ), 500

        # -------------------------------------------------
        # BUILD SHARE LINK DATA
        # -------------------------------------------------

        expiry_label = (
            get_s3_share_expiry_label(
                minutes
            )
        )

        generated_link = {
            "name": selected_file["name"],
            "key": key,
            "url": share_url,
            "expires_at": expiry_label,
            "validity": expiry_label
        }

        # -------------------------------------------------
        # IMPORTANT
        #
        # Direct S3 sharing does NOT require:
        #
        # share_token
        # shared_with
        # share_expires_at
        #
        # We therefore clear old application-token data
        # if those columns still exist in the database.
        # -------------------------------------------------

        try:

            connection.execute(
                """
                UPDATE files
                SET shared_with = NULL,
                    share_token = NULL,
                    share_expires_at = NULL
                WHERE id = ?
                AND user_id = ?
                """,
                (
                    file_record["id"],
                    session["user_id"]
                )
            )

            connection.commit()

        except Exception as database_error:

            # If these legacy sharing columns are missing,
            # the S3 URL itself is still valid.
            print(
                "SHARE DATABASE CLEANUP WARNING:",
                database_error
            )

            try:
                connection.rollback()
            except Exception:
                pass

        # -------------------------------------------------
        # JSON RESPONSE
        # -------------------------------------------------

        if request.is_json:

            return jsonify({
                "success": True,
                "message": (
                    "Amazon S3 sharing link generated successfully."
                ),
                "name": selected_file["name"],
                "key": key,
                "url": share_url,
                "expires_at": expiry_label,
                "validity": expiry_label,
                "minutes": minutes
            })

        # -------------------------------------------------
        # NORMAL HTML RESPONSE
        # -------------------------------------------------

        flash(
            "Amazon S3 sharing link generated successfully.",
            "success"
        )

        return render_template(
            "share.html",

            files=selected_files,

            name=selected_file["name"],

            share_url=share_url,

            share_links=[
                generated_link
            ],

            generated_links=[
                generated_link
            ],

            minutes=minutes,

            shared_with="",

            failed_files=[]
        )

    except Exception as error:

        print(
            "SHARE ERROR:",
            error
        )

        try:
            connection.rollback()
        except Exception:
            pass

        if request.is_json:

            return jsonify({
                "success": False,
                "message": (
                    "Unable to generate the Amazon S3 "
                    "sharing link."
                )
            }), 500

        flash(
            "Unable to generate the Amazon S3 sharing link.",
            "error"
        )

        return render_template(
            "share.html",
            files=[],
            name="",
            share_url=None,
            share_links=[],
            generated_links=[],
            minutes=15,
            shared_with="",
            failed_files=[]
        ), 500

    finally:

        connection.close()


# =========================================================
# BULK SHARE
# =========================================================

@app.route(
    "/bulk-share",
    methods=["GET", "POST"]
)
@login_required
def bulk_share():

    # -----------------------------------------------------
    # GET
    # -----------------------------------------------------

    if request.method == "GET":

        return render_template(
            "share.html",

            files=[],

            name="",

            share_url=None,

            share_links=[],

            generated_links=[],

            minutes=15,

            shared_with="",

            failed_files=[]
        )

    # -----------------------------------------------------
    # GET SELECTED FILE KEYS
    # -----------------------------------------------------

    keys = get_selected_file_keys()

    if not keys:

        if request.is_json:

            return jsonify({
                "success": False,
                "message": (
                    "Please select at least one file."
                )
            }), 400

        flash(
            "Please select at least one file to share.",
            "error"
        )

        return redirect(
            request.referrer
            or url_for("dashboard")
        )

    # -----------------------------------------------------
    # DATABASE
    # -----------------------------------------------------

    connection = get_db()

    try:

        placeholders = ",".join(
            ["?"] * len(keys)
        )

        rows = connection.execute(
            f"""
            SELECT *
            FROM files
            WHERE user_id = ?
            AND s3_key IN ({placeholders})
            AND deleted = 0
            AND file_type != 'folder'
            """,
            [session["user_id"]] + keys
        ).fetchall()

        rows_by_key = {
            row["s3_key"]: row
            for row in rows
        }

        # -------------------------------------------------
        # BUILD SELECTED FILES
        # -------------------------------------------------

        selected_files = []

        for selected_key in keys:

            row = rows_by_key.get(
                selected_key
            )

            if not row:
                continue

            selected_files.append(
                build_share_file_data(
                    row
                )
            )

        # -------------------------------------------------
        # NO VALID FILES
        # -------------------------------------------------

        if not selected_files:

            flash(
                "None of the selected files are available.",
                "error"
            )

            return redirect(
                request.referrer
                or url_for("dashboard")
            )

        # -------------------------------------------------
        # EXPIRY
        # -------------------------------------------------

        minutes, expiry_error = (
            get_requested_share_minutes()
        )

        if expiry_error:

            if request.is_json:

                return jsonify({
                    "success": False,
                    "message": expiry_error
                }), 400

            flash(
                expiry_error,
                "error"
            )

            return render_template(
                "share.html",

                files=selected_files,

                name=selected_files[0]["name"],

                share_url=None,

                share_links=[],

                generated_links=[],

                minutes=15,

                shared_with="",

                failed_files=[]
            ), 400

        # -------------------------------------------------
        # GENERATE LINKS
        # -------------------------------------------------

        generated_links = []

        failed_files = []

        expiry_label = (
            get_s3_share_expiry_label(
                minutes
            )
        )

        for file_data in selected_files:

            key = file_data["key"]

            try:

                share_url = (
                    generate_direct_s3_share_url(
                        key,
                        minutes
                    )
                )

                generated_links.append({
                    "name": file_data["name"],
                    "key": key,
                    "url": share_url,
                    "expires_at": expiry_label,
                    "validity": expiry_label
                })

                # -------------------------------------------------
                # Clear old application-token sharing fields.
                #
                # The actual active sharing mechanism is now the
                # direct S3 presigned URL.
                # -------------------------------------------------

                try:

                    connection.execute(
                        """
                        UPDATE files
                        SET shared_with = NULL,
                            share_token = NULL,
                            share_expires_at = NULL
                        WHERE id = ?
                        AND user_id = ?
                        AND deleted = 0
                        AND file_type != 'folder'
                        """,
                        (
                            file_data["id"],
                            session["user_id"]
                        )
                    )

                except Exception as database_error:

                    print(
                        "BULK SHARE CLEANUP WARNING:",
                        database_error
                    )

                    try:
                        connection.rollback()
                    except Exception:
                        pass

            except ClientError as error:

                print(
                    "BULK SHARE S3 ERROR:",
                    key,
                    error
                )

                failed_files.append(
                    file_data["name"]
                )

            except Exception as error:

                print(
                    "BULK SHARE ERROR:",
                    key,
                    error
                )

                failed_files.append(
                    file_data["name"]
                )

        # -------------------------------------------------
        # NO LINKS GENERATED
        # -------------------------------------------------

        if not generated_links:

            try:
                connection.rollback()
            except Exception:
                pass

            if request.is_json:

                return jsonify({
                    "success": False,
                    "message": (
                        "Unable to create S3 share links "
                        "for the selected files."
                    ),
                    "failed_files": failed_files
                }), 500

            flash(
                "Unable to create S3 share links "
                "for the selected files.",
                "error"
            )

            return render_template(
                "share.html",

                files=selected_files,

                name=selected_files[0]["name"],

                share_url=None,

                share_links=[],

                generated_links=[],

                minutes=minutes,

                shared_with="",

                failed_files=failed_files
            ), 500

        # -------------------------------------------------
        # SAVE DATABASE CHANGES
        # -------------------------------------------------

        try:

            connection.commit()

        except Exception as database_error:

            connection.rollback()

            print(
                "BULK SHARE COMMIT ERROR:",
                database_error
            )

            # The S3 URLs have already been generated, but the
            # old database cleanup could not be saved.
            # Do not destroy the generated links.

        # -------------------------------------------------
        # JSON RESPONSE
        # -------------------------------------------------

        if request.is_json:

            return jsonify({
                "success": True,
                "message": (
                    f"{len(generated_links)} file(s) "
                    "shared successfully."
                ),
                "links": generated_links,
                "failed_files": failed_files,
                "minutes": minutes,
                "validity": expiry_label
            })

        # -------------------------------------------------
        # FLASH MESSAGE
        # -------------------------------------------------

        if failed_files:

            flash(
                f"{len(generated_links)} file(s) shared successfully. "
                f"{len(failed_files)} file(s) could not be shared.",
                "success"
            )

        else:

            flash(
                f"{len(generated_links)} file(s) "
                "shared successfully.",
                "success"
            )

        # -------------------------------------------------
        # HTML RESPONSE
        # -------------------------------------------------

        return render_template(
            "share.html",

            files=selected_files,

            name=selected_files[0]["name"],

            share_url=(
                generated_links[0]["url"]
                if len(generated_links) == 1
                else None
            ),

            share_links=generated_links,

            generated_links=generated_links,

            minutes=minutes,

            shared_with="",

            failed_files=failed_files
        )

    except Exception as error:

        try:
            connection.rollback()
        except Exception:
            pass

        print(
            "BULK SHARE DATABASE ERROR:",
            error
        )

        if request.is_json:

            return jsonify({
                "success": False,
                "message": (
                    "Unable to process the selected files."
                )
            }), 500

        flash(
            "Unable to process the selected files.",
            "error"
        )

        return render_template(
            "share.html",

            files=[],

            name="",

            share_url=None,

            share_links=[],

            generated_links=[],

            minutes=15,

            shared_with="",

            failed_files=[]
        ), 500

    finally:

        connection.close()



# DELETE FOLDER
# =========================================================

@app.route(
    "/delete-folder",
    methods=["POST"]
)
@login_required
def delete_folder():

    key = request.form.get(
        "key",
        ""
    )

    root = user_folder()

    if not key.startswith(root):

        return "Access Denied", 403

    key = normalize_folder(
        key
    )

    if key == root:

        return "Cannot delete root folder.", 400

    connection = get_db()

    folder_record = connection.execute(
        """
        SELECT *

        FROM files

        WHERE user_id = ?

        AND s3_key = ?

        AND file_type = 'folder'
        """,
        (
            session["user_id"],
            key
        )
    ).fetchone()

    if not folder_record:

        folder_name = (
            key.rstrip("/")
            .split("/")[-1]
        )

        parent_folder = (

            "/".join(
                key.rstrip("/")
                .split("/")[:-1]
            )

            + "/"

        )

        now = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        try:

            connection.execute(
                """
                INSERT INTO files
                (
                    user_id,
                    name,
                    s3_key,
                    folder,
                    size,
                    file_type,
                    created_at,
                    last_accessed,
                    starred,
                    deleted,
                    shared_with
                )

                VALUES
                (
                    ?,
                    ?,
                    ?,
                    ?,
                    0,
                    'folder',
                    ?,
                    ?,
                    0,
                    1,
                    NULL
                )
                """,
                (
                    session["user_id"],
                    folder_name,
                    key,
                    parent_folder,
                    now,
                    now
                )
            )

            connection.execute(
                """
                UPDATE files

                SET deleted = 1

                WHERE user_id = ?

                AND s3_key LIKE ?

                AND deleted = 0
                """,
                (
                    session["user_id"],
                    key + "%"
                )
            )

            connection.commit()

            connection.close()

            flash(
                "Folder moved to Trash.",
                "success"
            )

        except sqlite3.IntegrityError as error:

            connection.rollback()

            connection.close()

            print(
                "DELETE FOLDER DATABASE ERROR:",
                error
            )

            flash(
                "Unable to move folder to Trash.",
                "error"
            )

        parent = (

            "/".join(
                key.rstrip("/")
                .split("/")[:-1]
            )

            + "/"

        )

        if not parent.startswith(root):

            parent = root

        return redirect(

            url_for(

                "dashboard",

                folder=parent

            )

        )

    try:

        connection.execute(
            """
            UPDATE files

            SET deleted = 1

            WHERE user_id = ?

            AND s3_key LIKE ?

            AND deleted = 0
            """,
            (
                session["user_id"],
                key + "%"
            )
        )

        connection.commit()

        connection.close()

        flash(
            "Folder moved to Trash.",
            "success"
        )

    except Exception as error:

        connection.close()

        print(
            "DELETE FOLDER ERROR:",
            error
        )

        flash(
            "Folder deletion failed: "
            + str(error),
            "error"
        )

    parent = (

        "/".join(
            key.rstrip("/")
            .split("/")[:-1]
        )

        + "/"

    )

    if not parent.startswith(root):

        parent = root

    return redirect(

        url_for(

            "dashboard",

            folder=parent

        )

    )


# =========================================================
# APPLICATION START
# =========================================================

if __name__ == "__main__":

    init_database()

    print()

    print("=" * 60)

    print(
        "CLOUD FILE STORAGE SYSTEM"
    )

    print("=" * 60)

    print(
        f"AWS Region: {AWS_REGION}"
    )

    print(
        f"S3 Bucket: {S3_BUCKET}"
    )

    print(
        "AWS Credentials: "
        +
        (
            "CONFIGURED"

            if
            AWS_ACCESS_KEY_ID
            and
            AWS_SECRET_ACCESS_KEY

            else

            "NOT CONFIGURED"
        )
    )

    print("=" * 60)

    print()

    if (
        AWS_ACCESS_KEY_ID
        and
        AWS_SECRET_ACCESS_KEY
        and
        S3_BUCKET
    ):

        try:

            s3.head_bucket(
                Bucket=S3_BUCKET
            )

            print(
                "AWS S3 Connection: SUCCESS"
            )

        except ClientError as error:

            print(
                "AWS S3 Connection: FAILED"
            )

            print(
                "AWS ERROR:",
                error
            )

    else:

        print(
            "AWS S3 Connection: SKIPPED"
        )

        print(
            "Please check your .env file."
        )

    print()

    app.run(

        debug=True,

        host="0.0.0.0",

        port=5000,

        threaded=True

    )