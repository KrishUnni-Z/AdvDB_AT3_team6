#!/usr/bin/env python3
# Generate hybrid streaming + e-commerce behavioural events as NDJSON, bucketed by hour folders (YYYY-MM-DD/HH).
# Example:
#   python generate_events_hybrid_cli.py --outdir data --hours 168 --sessions-per-hour 150

import argparse, json, random, uuid, os
from datetime import datetime, timedelta, timezone

def uid(): return str(uuid.uuid4())
def iso(ts): return ts.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
def to_date_str(ts): return ts.astimezone(timezone.utc).date().isoformat()
def hhmm(ts): return ts.hour*100 + ts.minute
def pick(seq): return random.choice(seq)

def write_ndjson(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows: f.write(json.dumps(r) + "\n")

def make_catalog(prefix, n):
    return [f"{prefix}_{i:04d}" for i in range(1, n+1)]

def envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id,
             content_id, event_type, ts, exp):
    return {
        "event_id": uid(),
        "event_type": event_type,
        "occurred_at": iso(ts),
        "received_at": iso(ts + timedelta(seconds=random.randint(0, args.max_lag_sec))),
        "date_id": to_date_str(ts),
        "time_id": hhmm(ts),
        "user_id": user_id,
        "household_id": household_id,
        "profile_id": profile_id,
        "session_id": session_id,
        "device_id": device_id,
        "location_id": location_id,
        "content_id": content_id,
        "country": pick(args.countries),
        "app_version": f"{random.randint(1,7)}.{random.randint(0,9)}.{random.randint(0,9)}",
        "experiment_id": exp["experiment_id"] if exp else None,
        "variant": exp["variant"] if exp else None
    }

def experiment():
    return random.choice([None, {"experiment_id":"exp_row_order_001","variant":random.choice(["A","B"])}])

def gen_hour(args, base_ts, USERS, PROFILES, HOUSEHOLDS, DEVICES, LOCATIONS, CONTENTS):
    sessions, engagement, search, browse, tile, commerce, notif = [], [], [], [], [], [], []
    n_sessions = max(1, int(args.sessions_per_hour))
    device_models = ["LG-WebOS","Samsung-Tizen","iPhone","Pixel","iPad","Chrome-Desktop"]
    surfaces = ["home","genre","search_results","editorial"]
    view_types = ["home","promo","recommendation","editorial"]
    query_pool = ["aliens","family","comedy","drama","space","crime","romance","heist","mystery"]

    for _ in range(n_sessions):
        user_id      = pick(USERS)
        profile_id   = pick(PROFILES)
        household_id = pick(HOUSEHOLDS)
        device_id    = pick(DEVICES)
        location_id  = pick(LOCATIONS)
        exp          = experiment()

        start = base_ts.replace(minute=random.randint(0,59), second=random.randint(0,59), microsecond=0)
        duration = random.randint(args.min_session_sec, args.max_session_sec)
        end = start + timedelta(seconds=duration)
        session_id = uid()
        content_id = pick(CONTENTS)

        env = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, content_id, "stream_session", start, exp)
        sessions.append({
            **env,
            "start_ts": env["occurred_at"],
            "end_ts": iso(end),
            "duration_sec": duration,
            "playhead_sec": random.randint(0, duration),
            "avg_bitrate_kbps": random.choice([None, random.randint(800, 4500)]),
            "rebuffer_count": random.choice([0,0,0,1,2]),
            "rebuffer_sec": random.choice([0,0,0,2,5,15,30]),
            "join_time_ms": random.choice([180,250,400,800,1200]),
            "network_type": random.choice(["wifi","4g","5g"]),
            "device_model": pick(device_models),
        })

        if random.random() < args.p_notif:
            sent_ts = start - timedelta(minutes=random.randint(5, 180))
            open_ts = None if random.random() < 0.4 else sent_ts + timedelta(minutes=random.randint(1, 90))
            envn = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, None, "notification", sent_ts, exp)
            notif.append({
                **envn,
                "notif_id": uid(),
                "channel": random.choice(["push","email","in_app"]),
                "template_id": random.choice(["tpl_reactivate","tpl_new_release","tpl_continue_watching","tpl_editorial"]),
                "campaign_id": random.choice(["cmp_oct_releases","cmp_returning_users","cmp_weekend_special"]),
                "sent_ts": envn["occurred_at"],
                "opened_ts": None if open_ts is None else iso(open_ts),
                "cta_clicked": False if open_ts is None else (random.random() < 0.6),
                "content_targeted_id": random.choice([None, pick(CONTENTS)]),
            })

        if random.random() < args.p_search:
            t = start + timedelta(seconds=random.randint(5, 180))
            envs = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, None, "search", t, exp)
            clicked = None if random.random() < 0.5 else pick(CONTENTS)
            search.append({
                **envs,
                "query": pick(query_pool),
                "results_returned": random.randint(0, 80),
                "clicked_content_id": clicked,
                "latency_ms": random.randint(25, 600),
            })

        t0 = start + timedelta(seconds=random.randint(10, 900))
        for row_idx in range(random.randint(1, 3)):
            envb = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, None, "browse", t0 + timedelta(seconds=row_idx*5), exp)
            browse.append({
                **envb,
                "surface": pick(surfaces),
                "view_type": pick(view_types),
                "nav_action": random.choice(["scroll","open_row","back"]),
                "rail_id": f"rail_{random.randint(1,80)}",
                "position": random.randint(1, 40),
                "impression_value": round(random.random()*2.0, 4),
            })
            for pos in range(1, random.randint(3,7)):
                c = pick(CONTENTS)
                ts = t0 + timedelta(seconds=row_idx*5 + pos)
                clicked = random.random() < args.p_tile_click
                will_convert = clicked and (random.random() < args.p_click_convert)
                latency = random.randint(1, args.conversion_window_sec) if will_convert else None
                envt = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, c, "tile_impression", ts, exp)
                tile.append({
                    **envt,
                    "tile_id": f"tile_{c}",
                    "row_id": f"row_{row_idx}",
                    "position": pos,
                    "dwell_ms": random.randint(50, 5000),
                    "preview_watch_ms": random.choice([0, random.randint(0, 20000)]),
                    "clicked": clicked,
                    "conversion_flag": bool(will_convert),
                    "conversion_latency_sec": latency,
                })
                if clicked:
                    enve = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, c, "engagement", ts + timedelta(seconds=1), exp)
                    engagement.append({
                        **enve,
                        "action": "play",
                        "playhead_sec": 0,
                        "funnel_stage": "conversion",
                        "extra": {"source":"tile_click"},
                    })
                    if random.random() < 0.6:
                        enve2 = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, c, "engagement", ts + timedelta(seconds=1+random.randint(5,120)), exp)
                        engagement.append({
                            **enve2,
                            "action": random.choice(["pause","stop","thumbs_up","skip_intro"]),
                            "playhead_sec": random.randint(5, 300),
                            "funnel_stage": "engagement",
                            "extra": {"note":"post-click behaviour"},
                        })

        if random.random() < args.p_commerce:
            pay_ts = start - timedelta(minutes=random.randint(10, 240))
            envc = envelope(args, user_id, profile_id, household_id, session_id, device_id, location_id, None, "commerce", pay_ts, exp)
            flow = random.choice(["view_pricing","start_checkout","payment_attempt","success","refund"])
            amount = random.choice([1099, 1399, 1799])
            commerce.append({
                **envc,
                "flow_step": flow,
                "amount_cents": amount if flow in ["payment_attempt","success","refund"] else None,
                "currency": "AUD",
                "plan_type": random.choice(["Basic","Standard","Premium"]),
                "payment_method": random.choice(["Card","PayPal","GiftCard"]),
                "promo_code": random.choice([None,"OCT10","WELCOME5","BINGE2"]),
                "renewal_flag": (random.random() < 0.5) if flow in ["payment_attempt","success"] else None,
                "failure_reason": None if flow in ["view_pricing","start_checkout","success","refund"] else random.choice([None,"card_declined","insufficient_funds"]),
            })

    # duplicate + late example for Silver tests
    if sessions:
        sessions.append(json.loads(json.dumps(sessions[0])))
    if engagement:
        late = json.loads(json.dumps(engagement[0]))
        occ = datetime.fromisoformat(late["occurred_at"].replace("Z","+00:00"))
        late["occurred_at"] = iso(occ - timedelta(minutes=15))
        late["date_id"] = late["occurred_at"][:10]
        late["received_at"] = iso(datetime.now(timezone.utc))
        engagement.append(late)

    return {
        "stream_session": sessions,
        "engagement": engagement,
        "search": search,
        "browse": browse,
        "tile_impression": tile,
        "commerce": commerce,
        "notification": notif,
    }

def main():
    ap = argparse.ArgumentParser(description="Generate Netflix-like behavioural events (hour buckets, NDJSON)")
    ap.add_argument("--outdir", default="data", help="Output root folder")
    ap.add_argument("--seed", type=int, default=13)
    ap.add_argument("--hours", type=int, default=24, help="Generate past N hours")
    ap.add_argument("--sessions-per-hour", type=float, default=50, help="Average sessions per hour")
    ap.add_argument("--users", type=int, default=60)
    ap.add_argument("--profiles", type=int, default=100)
    ap.add_argument("--households", type=int, default=40)
    ap.add_argument("--devices", type=int, default=60)
    ap.add_argument("--locations", type=int, default=60)
    ap.add_argument("--contents", type=int, default=120)
    ap.add_argument("--countries", nargs="*", default=["AU","NZ","US","IN","GB"])
    ap.add_argument("--min-session-sec", type=int, default=120)
    ap.add_argument("--max-session-sec", type=int, default=7200)
    ap.add_argument("--p-search", type=float, default=0.6)
    ap.add_argument("--p-tile-click", type=float, default=0.15)
    ap.add_argument("--p-click-convert", type=float, default=0.7)
    ap.add_argument("--p-commerce", type=float, default=0.12)
    ap.add_argument("--p-notif", type=float, default=0.25)
    ap.add_argument("--conversion-window-sec", type=int, default=900)
    ap.add_argument("--max-lag-sec", type=int, default=180, help="ingest lag seconds (received_at)")
    args = ap.parse_args()

    random.seed(args.seed)

    USERS      = make_catalog("u", args.users)
    PROFILES   = make_catalog("p", args.profiles)
    HOUSEHOLDS = make_catalog("h", args.households)
    DEVICES    = make_catalog("d", args.devices)
    LOCATIONS  = make_catalog("l", args.locations)
    CONTENTS   = make_catalog("c", args.contents)

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    totals = {k:0 for k in ["stream_session","engagement","search","browse","tile_impression","commerce","notification"]}

    for h in range(args.hours, 0, -1):
        base_ts = now - timedelta(hours=h-1)
        rows = gen_hour(args, base_ts, USERS, PROFILES, HOUSEHOLDS, DEVICES, LOCATIONS, CONTENTS)
        day = base_ts.strftime("%Y-%m-%d"); hour = base_ts.strftime("%H")
        outdir = os.path.join(args.outdir, day, hour)
        write_ndjson(os.path.join(outdir, "bronze_stream_session.ndjson"), rows["stream_session"])
        write_ndjson(os.path.join(outdir, "bronze_engagement_event.ndjson"), rows["engagement"])
        write_ndjson(os.path.join(outdir, "bronze_search_event.ndjson"), rows["search"])
        write_ndjson(os.path.join(outdir, "bronze_browse_event.ndjson"), rows["browse"])
        write_ndjson(os.path.join(outdir, "bronze_tile_impression.ndjson"), rows["tile_impression"])
        write_ndjson(os.path.join(outdir, "bronze_commerce_event.ndjson"), rows["commerce"])
        write_ndjson(os.path.join(outdir, "bronze_notification_event.ndjson"), rows["notification"])
        for k,v in rows.items(): totals[k] += len(v)

    print("=== Summary ===")
    for k,v in totals.items():
        print(f"{k:>16}: {v}")
    print(f"Output root: {os.path.abspath(args.outdir)}")

if __name__ == "__main__":
    main()
