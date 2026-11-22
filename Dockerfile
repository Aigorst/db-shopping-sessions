# Shopping Sessions: one static C++ binary in an empty image.
# Build time is not scored; the run starts a single process with no loader.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY src/ src/
ARG ARCH_FLAGS="-march=cascadelake -mtune=cascadelake"
RUN g++ -std=c++17 -O3 ${ARCH_FLAGS} -fno-plt -static -pthread -o /ss src/main.cpp

FROM scratch
COPY --from=build /ss /ss
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/ss"]
