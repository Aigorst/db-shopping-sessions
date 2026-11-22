# Shopping Sessions: static C++ binaries in an empty image.
# Build time is not scored; the run starts a single process with no loader.
#
# /ss is a portable x86-64-v3 (AVX2) build that runs on any Haswell-or-newer x86 CPU.
# If AVX512_FLAGS is set, a second build /ss-avx512 is added; /ss re-executes it at startup
# (one execve, well under 1 ms) only when CPUID reports the full x86-64-v4 + VNNI set, so the
# image never SIGILLs elsewhere. The AVX-512 build uses plain -march=cascadelake codegen (no
# -DSS_AVX512), the same configuration the scorer accepted at 34258b7. Disable with AVX512_FLAGS="".
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ src/
ARG ARCH_FLAGS="-march=x86-64-v3 -mtune=generic"
ARG AVX512_FLAGS="-march=cascadelake -mtune=cascadelake"
# -mbranches-within-32B-boundaries: keeps jumps off 32-byte boundaries, avoiding the Skylake/Cascade
# Lake JCC-erratum microcode penalty (as Cascade Lake teams do). Neutral on newer CPUs (measured locally).
ARG COMMON_FLAGS="-std=c++17 -O3 -fno-plt -static -pthread -Wa,-mbranches-within-32B-boundaries"
RUN mkdir /out \
 && g++ ${COMMON_FLAGS} ${ARCH_FLAGS} -o /out/ss src/main.cpp \
 && if [ -n "${AVX512_FLAGS}" ]; then g++ ${COMMON_FLAGS} ${AVX512_FLAGS} -o /out/ss-avx512 src/main.cpp; fi

FROM scratch
COPY --from=build /out/ /
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/ss"]
