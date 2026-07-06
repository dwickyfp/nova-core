# Agent Rules

## Parallel Work Safety

Karena perubahan kode dapat dilakukan oleh beberapa agent secara paralel, jangan menghapus, revert, atau mengedit perubahan yang bukan dibuat oleh task/session kamu sendiri meskipun perubahan tersebut muncul di `git diff`. Batasi edit hanya pada file dan bagian kode yang relevan dengan task kamu. Jika melihat diff yang tidak kamu buat, anggap itu milik agent lain dan biarkan tetap utuh kecuali user secara eksplisit meminta untuk mengubahnya.
