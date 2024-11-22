import glob
import json
import os
import re
import socket
import stat
import subprocess

import requests
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

PACKAGE_EMOJI = ":books:"
PACKAGE_NAME = "panda-common"


def get_user():
    # Run the 'klist' command and capture its output
    result = subprocess.run(["klist"], capture_output=True, text=True)

    # Filter the lines containing 'Default principal' and extract the last field
    for line in result.stdout.splitlines():
        if "Default principal" in line:
            # Split the line by spaces and get the last element (field)
            default_principal = line.split()[-1]
            default_principal = default_principal.split("@")[0]
            return default_principal

    return ""


def get_repo_info() -> object:
    # Get the current remote URL of the repository
    repo_url = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).strip().decode()

    # Get the repo and branch name
    match = re.match(r"https://github.com/(.*).git@(.*)", repo_url)

    if match:
        repo_name = match.group(1)
        branch_name = match.group(2)
    else:
        repo_name = repo_url.removesuffix(".git")
        branch_name = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode()

    # Commit hash
    commit_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode()

    return repo_name, branch_name, commit_hash


def mm_notification():
    # Environment variable to check if we should silence the notification
    if os.environ.get("DISABLE_MM"):
        return

    # Get user that is running the upgrade
    user = get_user()

    # Get repository information
    repo_name, branch_name, commit_hash = get_repo_info()

    # Get Server Name
    server_name = socket.gethostname()

    file_path = os.path.expanduser("~/mm_webhook_url.txt")
    with open(file_path, "r") as file:
        mm_webhook_url = file.read().strip()
        if not mm_webhook_url:
            return

    # On the repository name we enter an empty space to prevent the URLs to preview on Mattermost
    # We shorten the commit hash to the first seven characters, as they are usually enough to identify a commit
    mm_message = {
        "text": f"{PACKAGE_EMOJI}**{PACKAGE_NAME}@{branch_name} upgrade on:** `{server_name}` by `{user}`.",
        "props": {
            "card": f"""
| **Property** | **Value** |
|--------------|-----------|
| **Package**  | {repo_name} |
| **Branch**   | [`{branch_name}`]({repo_name}/tree/{branch_name}) |
| **Commit**   |  [`{commit_hash}`]({repo_name}/commit/{commit_hash}) |
"""
        },
    }
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(mm_webhook_url, data=json.dumps(mm_message), headers=headers)
    except requests.exceptions.RequestException as e:
        pass


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        # chmod +x
        for f in glob.glob("./tools/*"):
            st = os.stat(f)
            os.chmod(f, st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def finalize(self, version, build_data, artifact_path):
        # update the mattermost chat-ops channel
        try:
            mm_notification()
        except:
            pass
