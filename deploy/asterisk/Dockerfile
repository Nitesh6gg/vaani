# Builds Asterisk 22.8+ from source (no maintained official image exists for this
# version yet) with just the modules Vaani needs: PJSIP, ARI, and the Stasis app.
FROM debian:bookworm-slim AS builder

ARG ASTERISK_VERSION=22.8.0

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential wget ca-certificates git subversion \
    libxml2-dev libsqlite3-dev libssl-dev libjansson-dev \
    uuid-dev libedit-dev libncurses-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
RUN wget -q https://downloads.asterisk.org/pub/telephony/asterisk/asterisk-${ASTERISK_VERSION}.tar.gz \
    && tar xzf asterisk-${ASTERISK_VERSION}.tar.gz \
    && rm asterisk-${ASTERISK_VERSION}.tar.gz

WORKDIR /usr/src/asterisk-${ASTERISK_VERSION}
RUN contrib/scripts/install_prereq install \
    && ./configure --with-jansson-bundled \
    && make menuselect.makeopts \
    && menuselect/menuselect \
         --enable res_pjsip \
         --enable res_pjsip_session \
         --enable chan_pjsip \
         --enable res_ari \
         --enable res_ari_channels \
         --enable res_ari_bridges \
         --enable res_ari_applications \
         --enable res_stasis \
         --enable app_stasis \
         --enable res_rtp_asterisk \
         --enable res_http_websocket \
         menuselect.makeopts \
    && make -j"$(nproc)" \
    && make install \
    && make samples

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 libsqlite3-0 libssl3 libjansson4 libuuid1 libedit2 libncurses6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/lib/asterisk /usr/lib/asterisk
COPY --from=builder /usr/sbin/asterisk /usr/sbin/asterisk
COPY --from=builder /var/lib/asterisk /var/lib/asterisk
COPY --from=builder /etc/asterisk /etc/asterisk

# Vaani's own config replaces the samples above.
COPY *.conf /etc/asterisk/

EXPOSE 5060/udp 8088/tcp
EXPOSE 10000-19999/udp

CMD ["asterisk", "-f", "-vvv"]
