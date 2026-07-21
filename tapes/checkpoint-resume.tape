# Records the docs/guides/checkpoint-resume.md flow: injected mid-pagination failure,
# the surviving checkpoint, the resumed run, and both outcomes in the ledger.
# Run via tapes/README.md; the shell starts in /repo (the mounted checkout).
Require dlt-ops
Output "/out/checkpoint-resume.gif"
Set Shell "bash"
Set FontSize 13
Set Width 900
Set Height 520
Set Padding 20
Set TypingSpeed 50ms
Set Framerate 12

Sleep 500ms
Type "cp -R examples/basic_project /tmp/dlt-demo"
Enter
Wait
Type "cd /tmp/dlt-demo"
Enter
Wait
Sleep 500ms
Type "GITHUB_EVENTS_FAIL_AFTER_PAGE=3 dlt-ops pipeline run -s github_events_api -y"
Enter
Wait
Sleep 3s
Type "dlt-ops checkpoints list --pipeline github_events_api_pipeline"
Enter
Wait
Sleep 3.5s
Type "dlt-ops pipeline run -s github_events_api -y"
Enter
Wait
Sleep 3s
Type "dlt-ops pipeline status"
Enter
Wait
Sleep 5.5s
