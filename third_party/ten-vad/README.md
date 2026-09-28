# TEN VAD — vendored native library

Files vendored from [TEN-framework/ten-vad](https://github.com/TEN-framework/ten-vad)
(Apache License 2.0, see `LICENSE`), consumed by the cgo wrapper in
`internal/ai/agent/tenvad` (the `VAD_MODE=ten` barge-in detector; see
`internal/ai/agent/bargein.go` and `bargein_ten.go`).

| File | Source path in ten-vad |
|---|---|
| `include/ten_vad.h` | `include/ten_vad.h` |
| `lib/Linux/x64/libten_vad.so` | `lib/Linux/x64/libten_vad.so` |

Vendored from the `main` branch on 2026-09-28. The `.so` is Linux x86-64 only —
that is Vaani's only production target (deploy/asterisk builds on Debian); other
platforms fall back to the energy detector via the `tenvad` stub (see the
`//go:build` constraints in the wrapper).

To refresh: download both files from a newer ten-vad commit with

    curl -sL -o include/ten_vad.h https://raw.githubusercontent.com/TEN-framework/ten-vad/main/include/ten_vad.h
    curl -sL -o lib/Linux/x64/libten_vad.so https://raw.githubusercontent.com/TEN-framework/ten-vad/main/lib/Linux/x64/libten_vad.so

then verify the `.so` is still a real ELF binary (`file` should say `ELF 64-bit
LSB shared object, x86-64`) and run a live call with `VAD_MODE=ten`.
