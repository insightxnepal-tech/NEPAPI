# Create this Cursor Automation

Open **https://cursor.com/automations/new** while logged in as insightxnepal@gmail.com, then fill the form exactly as below and click **Save / Create**.

## Form values

| Field | Value |
|-------|--------|
| Name | `NEPSE 5pm post-market` |
| Trigger | Scheduled |
| Cron | `15 11 * * 1-5` |
| Timezone | UTC (this is **5:00 PM Nepal Time**, Mon–Fri) |
| Repository | `insightxnepal-tech/NEPAPI` (required — agent must clone the repo) |
| Branch | `main` |
| Environment | existing personal env for this repo |
| Tools | Memories on, Pull request creation on |
| Scope | Private / only you |
| Secrets | `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID` |

## Prompt (paste into the automation)

```
You are the NEPSE 5pm post-market desk for repository github.com/insightxnepal-tech/NEPAPI.

1. Confirm today is a NEPSE trading day in Asia/Kathmandu. From 2026-04-06 the week is Monday–Friday. If it is Saturday, Sunday, or a holiday (no new floorsheet), stop. Do not open a PR.

2. Fetch today's full floorsheet:
   python fetch_floorsheet_csv.py --date $(TZ=Asia/Kathmandu date +%F) --out floorsheet_$(TZ=Asia/Kathmandu date +%F).csv
   If fetch fails, reuse floorsheet_YYYY-MM-DD.csv only when its businessDate is today. Otherwise stop.

3. Run the ranked strategy and Telegram send:
   python postmarket_strategy.py --skip-fetch
   Requires TELEGRAM_TOKEN and TELEGRAM_CHAT_ID. If they are missing, still write the markdown report and note that Telegram was skipped.

4. The script already walk-forward tests four models (continuation, smart-money, sniper, composite), picks the highest historical hit-rate model, writes strategy_postmarket_YYYY-MM-DD.md, and sends the Telegram briefing.

5. Commit floorsheet_YYYY-MM-DD.csv and strategy_postmarket_YYYY-MM-DD.md only if they changed. Open a PR only when there is a real data/report update. Do not rewrite unrelated files.

6. If Telegram fails, still save the markdown report and record the error. Never print bot tokens.
```

## After save

The automation should appear at https://cursor.com/automations and fire next weekday at 17:00 NPT.

GitHub Actions `.github/workflows/postmarket_strategy.yml` is the same 5pm job. Disable one of them if you want a single Telegram message per day.
