#!/usr/bin/env python3

import os
import argparse
import requests
import chess.pgn
import zstandard as zstd
import io
import csv
import time

# =========================
# CONSTANTS
# =========================
INDEX_URL = "https://database.lichess.org/standard/list.txt"
CHUNK_SIZE = 1024 * 1024
REQUEST_TIMEOUT = 10
PROGRESS_EVERY = 100_000  # progress step

# =========================
# ARGUMENTS
# =========================
def parse_args():
    p = argparse.ArgumentParser(
        description="Stream and filter huge Lichess PGN files safely"
    )
    p.add_argument("-o", "--output-folder", default="LichessDatabase")
    p.add_argument("--min-elo", type=int, default=2200)
    p.add_argument("--max-games", type=int, default=None)
    p.add_argument("--downloads", type=int, default=None)
    return p.parse_args()

# =========================
# RESUME HELPERS
# =========================
def load_processed_files(path):
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip()}

def mark_file_processed(path, name):
    with open(path, "a") as f:
        f.write(name + "\n")

# =========================
# CSV SETUP
# =========================
def setup_output(folder):
    os.makedirs(folder, exist_ok=True)
    csv_path = os.path.join(folder, "chess_games_filtered.csv")
    if not os.path.exists(csv_path):
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["game_id", "fen", "move", "result"])
    return csv_path

# =========================
# PGN URL LIST
# =========================
def iter_pgn_urls(index_url):
    with requests.get(index_url, stream=True, timeout=REQUEST_TIMEOUT) as r:
        r.raise_for_status()
        for line in r.iter_lines(decode_unicode=True):
            if line and line.endswith(".pgn.zst"):
                yield line.strip()

# =========================
# DOWNLOAD (SKIP IF EXISTS)
# =========================
def download_file(url, tmp_dir):
    name = url.split("/")[-1]
    path = os.path.join(tmp_dir, name)

    if os.path.exists(path):
        return path

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
    return path

# =========================
# MANUAL HEADER READER
# =========================
def read_headers_only(stream):
    headers = {}
    while True:
        line = stream.readline()
        if not line or line.strip() == "":
            break
        if line.startswith("["):
            key, val = line[1:-2].split(" ", 1)
            headers[key] = val.strip('"')
    return headers

def skip_moves(stream):
    while True:
        line = stream.readline()
        if not line or line.strip() == "":
            return

def headers_pass(headers, min_elo):
    w = headers.get("WhiteElo")
    b = headers.get("BlackElo")
    e = headers.get("Event")
    return (
        w and b and
        w.isdigit() and b.isdigit() and
        int(w) >= min_elo and
        int(b) >= min_elo and
        e == "Rated Classical game"
    )

# =========================
# STREAM PROCESSOR
# =========================
def process_pgn_file(
    pgn_path,
    csv_path,
    min_elo,
    max_games,
    start_game_id
):
    game_id = start_game_id
    processed = 0
    last_log = time.time()

    with open(csv_path, "a", newline="") as csv_file:
        writer = csv.writer(csv_file)

        with open(pgn_path, "rb") as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as reader:
                stream = io.TextIOWrapper(reader, encoding="utf-8", newline="")

                while True:
                    headers = read_headers_only(stream)
                    if not headers:
                        break

                    game_id += 1
                    processed += 1

                    if headers_pass(headers, min_elo):
                        game = chess.pgn.read_game(stream)
                        board = chess.Board()
                        for move in game.mainline_moves():
                            writer.writerow([
                                game_id,
                                board.fen(),
                                move.uci(),
                                headers.get("Result")
                            ])
                            board.push(move)
                    else:
                        skip_moves(stream)

                    if processed % PROGRESS_EVERY == 0:
                        print(f"Processed {processed:,} games")

                    if max_games and game_id >= max_games:
                        return game_id

    return game_id

# =========================
# MAIN
# =========================
def main():
    args = parse_args()

    csv_path = setup_output(args.output_folder)
    tmp_dir = os.path.join(args.output_folder, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    state_file = os.path.join(args.output_folder, "processed_files.txt")
    processed_files = load_processed_files(state_file)

    game_id = 0
    downloaded = 0

    print(f"Resuming from {len(processed_files)} completed files")

    for url in iter_pgn_urls(INDEX_URL):
        if args.downloads and downloaded >= args.downloads:
            break

        name = url.split("/")[-1]
        if name in processed_files:
            continue

        print(f"\nDownloading: {name}")
        path = download_file(url, tmp_dir)
        downloaded += 1

        game_id = process_pgn_file(
            path,
            csv_path,
            args.min_elo,
            args.max_games,
            game_id
        )

        mark_file_processed(state_file, name)
        processed_files.add(name)

        os.remove(path)
        print(f"Removed: {name}")

        if args.max_games and game_id >= args.max_games:
            break

    print(f"\nFinished. Total games processed: {game_id:,}")

# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    main()
