# Video studio

A desktop for making videos with coding agents: Claude Code and Codex with
[HyperFrames](https://hyperframes.heygen.com), which renders HTML compositions
to video, and [Remotion](https://www.remotion.dev), which renders React. Both
come with their agent skills and a starter project, next to the FFmpeg and
Google Chrome every Beam desktop has.

```sh
beam vm new video-studio --template TEMPLATE_ID    # the ID is on beam.cloud/templates
beam vm terminal video-studio                       # sign in to claude or codex, then ask
```

The template ships signed out of `claude` and `codex`. Ask them for a video,
such as "make a 10-second launch video for my app in /workspace/hyperframes",
and they write the composition and render it:

```sh
cd /workspace/hyperframes && hyperframes render -o launch.mp4
cd /workspace/remotion && npx remotion render MyComp out/video.mp4
```

`npm run dev` in either project starts a live preview that reloads as the agent
edits: HyperFrames on port 3002 and Remotion Studio on 3000. Open one on your
machine with `beam vm port-forward video-studio 3002`, then
http://127.0.0.1:3002. Download renders with
`beam vm scp video-studio:/workspace/hyperframes/launch.mp4 .`, or play them on
the desktop (`beam vm desktop video-studio`).

Renders run on the CPU: the HyperFrames starter's 10 seconds render in about 3,
and longer or heavier compositions take proportionally longer. Remotion
is free for individuals and companies of up to three employees; larger ones
need a [company license](https://www.remotion.dev/license).

Rebuild the template with `python sdk/examples/vm/templates/publish.py video-studio`.
