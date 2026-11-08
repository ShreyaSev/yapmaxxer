"""
Manage keyword rules and peek at the queue.

  python rules.py add "price" "Hey! Pricing is in your DMs."
  python rules.py add "link" "Link is in my bio." --media 1789xxxxxxxx --priority 10
  python rules.py list
  python rules.py disable 2
  python rules.py enable 2
  python rules.py events            # last 20 events and their status
"""

import argparse

import db


def cmd_add(conn, a):
    conn.execute(
        "INSERT INTO rules (keyword, reply, media_id, priority) VALUES (?, ?, ?, ?)",
        (a.keyword, a.reply, a.media, a.priority),
    )
    print("added")


def cmd_list(conn, _):
    rows = conn.execute("SELECT * FROM rules ORDER BY id").fetchall()
    if not rows:
        print("no rules yet")
    for r in rows:
        scope = r["media_id"] or "all posts"
        flag = "" if r["active"] else "  (disabled)"
        print(f"[{r['id']}] '{r['keyword']}' -> {r['reply']!r}  scope={scope} prio={r['priority']}{flag}")


def cmd_toggle(conn, a, active):
    conn.execute("UPDATE rules SET active = ? WHERE id = ?", (active, a.id))
    print("updated")


def cmd_events(conn, _):
    rows = conn.execute(
        "SELECT comment_id, commenter_username, text, status, attempts, last_error "
        "FROM events ORDER BY received_at DESC LIMIT 20"
    ).fetchall()
    if not rows:
        print("no events yet")
    for r in rows:
        err = f"  [{r['last_error']}]" if r["last_error"] else ""
        print(f"{r['status']:<10} x{r['attempts']}  @{r['commenter_username']}: {r['text']!r}{err}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("add")
    s.add_argument("keyword")
    s.add_argument("reply")
    s.add_argument("--media", default=None)
    s.add_argument("--priority", type=int, default=0)

    sub.add_parser("list")
    sub.add_parser("events")
    for name in ("enable", "disable"):
        s = sub.add_parser(name)
        s.add_argument("id", type=int)

    a = p.parse_args()
    db.init()
    conn = db.connect()
    try:
        if a.cmd == "add":
            cmd_add(conn, a)
        elif a.cmd == "list":
            cmd_list(conn, a)
        elif a.cmd == "events":
            cmd_events(conn, a)
        elif a.cmd == "enable":
            cmd_toggle(conn, a, 1)
        elif a.cmd == "disable":
            cmd_toggle(conn, a, 0)
    finally:
        conn.close()


if __name__ == "__main__":
    main()