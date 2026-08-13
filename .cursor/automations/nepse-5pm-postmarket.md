# Cursor Automation: NEPSE 5pm post-market

Create this automation at https://cursor.com/automations

## Trigger

- Type: Scheduled
- Cron (UTC): `15 11 * * 1-5`
- Equivalent: **5:00 PM Nepal Time, Monday–Friday** (NPT = UTC+05:45)
- Repository: `insightxnepal-tech/NEPAPI` (this repo)
- Skip Saturday and Sunday. The agent must still exit cleanly on NEPSE public holidays.

## Prompt (paste into the automation)

You are the NEPSE 5pm post-market desk for this repository.

1. Confirm today is a NEPSE trading day in Asia/Kathmandu. From 2026-04-06 the week is Monday–Friday. If it is Saturday, Sunday, or a holiday (no new floorsheet), stop. Do not open a PR.

2. Fetch today's full floorsheet:
   `python fetch_floorsheet_csv.py --date $(TZ=Asia/Kathmandu date +%F) --out floorsheet_$(TZ=Asia/Kathmandu date +%F).csv`
   If fetch fails, reuse `floorsheet_YYYY-MM-DD.csv` only when its `businessDate` is today. Otherwise stop.

3. Run the ranked strategy and Telegram send:
   `python postmarket_strategy.py --skip-fetch`
   Requires `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` in the environment.

4. The script already:
   - Walk-forward tests four models (continuation, smart-money, sniper, composite)
   - Picks the highest historical hit-rate model
   - Writes `strategy_postmarket_YYYY-MM-DD.md`
   - Sends the Telegram briefing

5. Commit `floorsheet_YYYY-MM-DD.csv` and `strategy_postmarket_YYYY-MM-DD.md` only if they changed. Open a PR only when there is a real data/report update. Do not rewrite unrelated files.

6. If Telegram fails, still save the markdown report and record the error. Never print bot tokens.

## Secrets

- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`

## Notes

GitHub Actions workflow `.github/workflows/postmarket_strategy.yml` is the primary scheduler. This Cursor automation is the agent-backed equivalent; do not enable both if you want a single Telegram message per day.
