#!/usr/bin/env python3
"""
Create the Team 4 backlog in GitHub from issues.json using the GitHub CLI (gh).

What it does (safe to re-run: existing labels, milestones and issues are skipped):
  1. creates labels (phase-N, role:PN, priority:PN, type:X)
  2. creates one milestone per phase with its due date
  3. creates the issues in dependency order, replacing T-ids with real #numbers
  4. (optional) adds each issue to your GitHub Project and sets Status, Priority, Phase, Estimate

Prerequisites:
  gh auth login
  gh auth refresh -s project          # only needed for step 4

Usage:
  python create_issues.py --repo ORG/Supply_Chain    --dry-run
  python create_issues.py --repo ORG/Supply_Chain    \
      --project 1 --project-owner ORG \
      --user P1=Elias --user P2=Anastasia --user P3=Christos --user P4=Thanos --user P5=you
"""
import argparse, json, re, subprocess, sys
from pathlib import Path

LABEL_COLORS = {"phase": "0E8A16", "role": "1D76DB", "priority": "B60205", "type": "C5DEF5"}
ROLE_NAMES = {"P1": "Data Engineering", "P2": "Analytics", "P3": "AI & API",
              "P4": "Cloud & DevOps", "P5": "Project Manager"}


def gh(args, dry=False, input_text=None, check=True):
    if dry:
        print("  [dry-run] gh", " ".join(a if " " not in a else repr(a) for a in args[:8]), "...")
        return ""
    r = subprocess.run(["gh", *args], input=input_text, capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.exit(f"gh {' '.join(args[:4])} failed:\n{r.stderr}")
    return r.stdout.strip()


def body_for(i, reviewer, ref):
    deps = ", ".join(ref(x) for x in i["deps"]) or "None"
    ac = "\n".join(f"- [ ] {a}" for a in i["ac"])
    return (f"## What needs to be done\n{i['desc']}\n\n## Acceptance criteria\n{ac}\n\n"
            f"## Dependencies\nRequires: {deps}\n\n## Owner / Reviewer\n"
            f"Owner: {i['role']} ({ROLE_NAMES[i['role']]}) · Reviewer: {reviewer[i['role']]}\n\n"
            f"## Planning\nPriority: {i['priority']} · Phase: {i['phase']} · "
            f"Estimate: {i['est']}h · Target date: {i['target']}\n\n"
            f"<!-- backlog-id: {i['id']} -->\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True, help="OWNER/REPO")
    p.add_argument("--file", default=str(Path(__file__).with_name("issues.json")))
    p.add_argument("--project", type=int, help="GitHub Project number (from its URL)")
    p.add_argument("--project-owner", help="user or org that owns the Project")
    p.add_argument("--user", action="append", default=[], help="ROLE=github_login, e.g. P1=alice")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    data = json.loads(Path(a.file).read_text(encoding="utf-8"))
    users = dict(u.split("=", 1) for u in a.user)
    dry = a.dry_run

    # 1. labels
    print("Labels...")
    all_labels = sorted({l for i in data["issues"] for l in i["labels"]})
    for l in all_labels:
        kind = l.split(":")[0].split("-")[0]
        gh(["label", "create", l, "--repo", a.repo, "--color", LABEL_COLORS.get(kind, "EDEDED"), "--force"], dry)

    # 2. milestones
    print("Milestones...")
    existing = set()
    if not dry:
        existing = {m["title"] for m in json.loads(
            gh(["api", f"repos/{a.repo}/milestones?state=all&per_page=100"]) or "[]")}
    for title, due in data["milestones"].values():
        if title in existing:
            continue
        gh(["api", f"repos/{a.repo}/milestones", "-f", f"title={title}",
            "-f", f"due_on={due}T23:59:59Z"], dry)

    # 3. issues (skip any already created, detected by the hidden backlog-id marker)
    print("Issues...")
    numbers = {}
    if not dry:
        for it in json.loads(gh(["issue", "list", "--repo", a.repo, "--state", "all",
                                 "--limit", "1000", "--json", "number,body,url"]) or "[]"):
            m = re.search(r"backlog-id: (T\d+)", it.get("body") or "")
            if m:
                numbers[m.group(1)] = (it["number"], it["url"])
    titles = {i["id"]: i["title"] for i in data["issues"]}
    ref = lambda t: f"#{numbers[t][0]}" if t in numbers else f"{t} ({titles[t]})"

    created = []
    for i in data["issues"]:
        if i["id"] in numbers:
            print(f"  skip {i['id']} (already #{numbers[i['id']][0]})")
            continue
        args = ["issue", "create", "--repo", a.repo, "--title", i["title"],
                "--body-file", "-", "--milestone", i["milestone"]]
        for l in i["labels"]:
            args += ["--label", l]
        if users.get(i["role"]):
            args += ["--assignee", users[i["role"]]]
        url = gh(args, dry, input_text=body_for(i, data["reviewer"], ref))
        num = int(url.rstrip("/").split("/")[-1]) if url else 0
        numbers[i["id"]] = (num, url)
        created.append(i)
        print(f"  {i['id']} -> #{num} {i['title']}")

    # 4. project board
    if not (a.project and a.project_owner):
        print("Done (no --project given, board step skipped).")
        return
    print("Project board...")
    if dry:
        print("  [dry-run] would add issues and set Status/Priority/Phase/Estimate")
        return
    proj = json.loads(gh(["project", "view", str(a.project), "--owner", a.project_owner, "--format", "json"]))
    fields = json.loads(gh(["project", "field-list", str(a.project), "--owner", a.project_owner,
                            "--format", "json", "--limit", "50"]))["fields"]
    by_name = {f["name"].lower(): f for f in fields}

    def option(field_name, matcher):
        f = by_name.get(field_name)
        if not f or "options" not in f:
            return None, None
        for o in f["options"]:
            if matcher(o["name"]):
                return f["id"], o["id"]
        return f["id"], None

    for i in data["issues"]:
        num, url = numbers[i["id"]]
        item = json.loads(gh(["project", "item-add", str(a.project), "--owner", a.project_owner,
                              "--url", url, "--format", "json"]))
        edits = [option("status", lambda n: n.strip().lower() == "backlog"),
                 option("priority", lambda n, p=i["priority"]: n.upper().startswith(p)),
                 option("phase", lambda n, ph=i["phase"]: re.search(rf"\b{ph}\b", n) is not None)]
        for fid, oid in edits:
            if fid and oid:
                gh(["project", "item-edit", "--id", item["id"], "--project-id", proj["id"],
                    "--field-id", fid, "--single-select-option-id", oid], check=False)
        est = by_name.get("estimate")
        if est:
            gh(["project", "item-edit", "--id", item["id"], "--project-id", proj["id"],
                "--field-id", est["id"], "--number", str(i["est"])], check=False)
        print(f"  board: #{num}")
    print("Done.")


if __name__ == "__main__":
    main()
