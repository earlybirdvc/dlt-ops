# Records the docs/getting-started/quickstart.md loop: scaffold -> validate -> run -> status.
# Run via tapes/README.md; the shell starts in /tmp inside the runner container.
Require dlt-ops
Output "/out/quickstart.gif"
Set Shell "bash"
Set FontSize 13
Set Width 900
Set Height 520
Set Padding 20
Set TypingSpeed 50ms
Set Framerate 12

Sleep 500ms
Type "dlt-ops init demo --example"
Enter
Wait
Sleep 2s
Type "cd demo"
Enter
Wait
Sleep 500ms
Type "dlt-ops pipeline validate"
Enter
Wait
Sleep 2s
Type "dlt-ops pipeline run -s demo_events -y"
Enter
Wait
Sleep 3s
Type "dlt-ops pipeline status"
Enter
Wait
Sleep 5s
