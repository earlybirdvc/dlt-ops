# Records the docs/guides/backfill.md flow: chunked backfill with the fault armed
# for chunk 3's window (the page's WEB_EVENTS_FAIL_FROM hook), then the re-run that
# skips completed chunks, retries the failed one, and runs the pending tail.
# Project setup (scaffold + the page's web_events source) happens off-screen via
# tapes/backfill_setup.sh. Run via tapes/README.md; the shell starts in /tmp.
Require dlt-ops
Output "/out/backfill.gif"
Set Shell "bash"
Set FontSize 13
Set Width 900
Set Height 520
Set Padding 20
Set TypingSpeed 40ms
Set Framerate 12

Hide
Type "bash /repo/tapes/backfill_setup.sh && cd demo && clear"
Enter
Wait
Show

Sleep 500ms
Type "dlt-ops pipeline validate"
Enter
Wait
Sleep 2s
Type `WEB_EVENTS_FAIL_FROM="2026-01-03T00:00:00+00:00" dlt-ops pipeline backfill web_events --from 2026-01-01T00:00:00Z --to 2026-01-06T00:00:00Z --chunk 1d`
Enter
Wait
Sleep 3.5s
Type "dlt-ops pipeline backfill web_events --from 2026-01-01T00:00:00Z --to 2026-01-06T00:00:00Z --chunk 1d"
Enter
Wait
Sleep 6s
