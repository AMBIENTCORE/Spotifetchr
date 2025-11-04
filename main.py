# Spotifetchr - Tkinter GUI app to fetch a user's public Spotify playlists and export tracks
# Requires: spotipy, pandas, openpyxl
#
# Install:
#   pip install spotipy pandas openpyxl
#
# Run:
#   python spotifetchr.py
#
# Notes:
# - Uses Client Credentials Flow (no redirect URI needed).
# - Stores Client ID/Secret and last username in ~/.spotifetchr.json (plaintext).
# - Filters playlists to those OWNED by the target user (created by that user).
# - Handles 429 rate limiting via Retry-After header with automatic backoff and resume.
#
# Styling mirrors user's dark Tkinter/ttk setup from their attached main.py.

import os
import json
import time
import threading
import queue
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# Data export
import pandas as pd

# Spotify
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".spotifetchr.json")


@dataclass
class TrackRow:
    artist: str
    title: str
    album: str
    playlist: str


class SpotifyWorker(threading.Thread):
    """
    Background thread that fetches playlists and tracks, with 429 handling.
    Communicates via a Queue with ('status', payload) messages for the GUI.
    """
    def __init__(self, user_id: str, client_id: str, client_secret: str, outq: queue.Queue):
        super().__init__(daemon=True)
        self.user_id = user_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.q = outq
        self._stop_flag = threading.Event()
        self.sp = None

    def stop(self):
        self._stop_flag.set()

    def run(self):
        try:
            self._auth()
            if self._stop_flag.is_set():
                return

            # Step 1: Fetch all public playlists for the user
            playlists = self._fetch_all_user_playlists(self.user_id)
            # Filter to those CREATED by that user (owner id equals the username requested)
            playlists = [pl for pl in playlists if (pl.get("owner", {}).get("id") or "").lower() == self.user_id.lower()]

            if not playlists:
                self.q.put(("message", f"No public playlists created by '{self.user_id}' were found."))
                self.q.put(("done", []))
                return

            # Pre-compute total track count for progress bar
            total_tracks = 0
            for pl in playlists:
                # 'tracks' contains total count
                total_tracks += pl.get("tracks", {}).get("total", 0)

            self.q.put(("progress_total", max(1, total_tracks)))
            processed = 0

            rows: List[TrackRow] = []

            # Step 2: For each playlist, fetch all items
            for pl in playlists:
                if self._stop_flag.is_set():
                    return
                pl_id = pl["id"]
                pl_name = pl.get("name", "Unknown Playlist")
                for item in self._paged_playlist_items(pl_id):
                    if self._stop_flag.is_set():
                        return
                    track = item.get("track")
                    if not track or track.get("type") != "track":
                        processed += 1
                        self.q.put(("progress", processed))
                        continue

                    # Extract artist names, title, album
                    artists = ", ".join(a.get("name", "") for a in track.get("artists", []) if a)
                    title = track.get("name", "")
                    album = (track.get("album") or {}).get("name", "")

                    rows.append(TrackRow(artist=artists or "Unknown Artist",
                                         title=title or "Unknown Title",
                                         album=album or "Unknown Album",
                                         playlist=pl_name))

                    processed += 1
                    if processed % 20 == 0 or processed == total_tracks:
                        self.q.put(("progress", processed))

            # Initial grouping by album: sort by album then artist/title
            rows.sort(key=lambda r: (r.album.lower(), r.artist.lower(), r.title.lower()))
            self.q.put(("done", rows))

        except Exception as e:
            self.q.put(("error", str(e)))

    # ---------- Spotify helpers with 429 handling ----------

    def _auth(self):
        auth_mgr = SpotifyClientCredentials(client_id=self.client_id, client_secret=self.client_secret)
        self.sp = spotipy.Spotify(auth_manager=auth_mgr, requests_timeout=20, retries=0)  # manual 429 handling

    def _handle_429(self, ex: SpotifyException):
        # Wait as long as the Retry-After header suggests; default to 5 seconds
        retry_after = 5
        try:
            if hasattr(ex, "http_status") and ex.http_status == 429:
                headers = getattr(ex, "http_headers", None) or {}
                retry = headers.get("Retry-After") or headers.get("retry-after")
                if retry is not None:
                    retry_after = int(retry)
        except Exception:
            pass
        # Inform GUI
        self.q.put(("message", f"Rate limited (429). Waiting {retry_after}s…"))
        slept = 0
        while slept < retry_after and not self._stop_flag.is_set():
            time.sleep(1)
            slept += 1

    def _safe_call(self, func, *args, **kwargs):
        while not self._stop_flag.is_set():
            try:
                return func(*args, **kwargs)
            except SpotifyException as ex:
                if getattr(ex, "http_status", None) == 429:
                    self._handle_429(ex)
                    continue  # retry
                else:
                    raise
            except Exception:
                raise

    def _fetch_all_user_playlists(self, user_id: str) -> List[Dict[str, Any]]:
        playlists: List[Dict[str, Any]] = []
        results = self._safe_call(self.sp.user_playlists, user_id, limit=50)
        while True:
            playlists.extend(results.get("items", []))
            next_url = results.get("next")
            if not next_url or self._stop_flag.is_set():
                break
            results = self._safe_call(self.sp.next, results)
        self.q.put(("message", f"Found {len(playlists)} playlists (before filtering by owner)."))
        return playlists

    def _paged_playlist_items(self, playlist_id: str):
        results = self._safe_call(self.sp.playlist_items, playlist_id, limit=100, additional_types=["track"])
        while True:
            for item in results.get("items", []):
                yield item
            next_url = results.get("next")
            if not next_url or self._stop_flag.is_set():
                break
            results = self._safe_call(self.sp.next, results)


class SpotifetchrApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Spotifetchr")
        try:
            self.state('zoomed')  # Windows
        except Exception:
            self.geometry("1200x800")

        # Dark theme styling similar to user's file
        self.configure(bg='#2b2b2b')
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure('TFrame', background='#2b2b2b')
        self.style.configure('TLabel', background='#2b2b2b', foreground='#ffffff')
        self.style.configure('TButton', background='#404040', foreground='#ffffff')
        self.style.configure('Treeview', background='#3c3c3c', foreground='#ffffff', fieldbackground='#3c3c3c')
        self.style.configure('Treeview.Heading', background='#404040', foreground='#ffffff')
        self.style.configure('TScrollbar', background='#404040')
        self.style.configure('TProgressbar', background='#00ff00', troughcolor='#2b2b2b')

        self.worker: Optional[SpotifyWorker] = None
        self.q: "queue.Queue" = queue.Queue()
        self.rows: List[TrackRow] = []

        # Load config
        self.config_data = self._load_config()

        # ------- Top bar: credentials & actions -------
        top = ttk.Frame(self)
        top.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)

        # Credentials
        ttk.Label(top, text="Client ID:").pack(side=tk.LEFT, padx=(0, 6))
        self.client_id_var = tk.StringVar(value=self.config_data.get("client_id", ""))
        ttk.Entry(top, textvariable=self.client_id_var, width=40).pack(side=tk.LEFT)

        ttk.Label(top, text="Client Secret:", padding=(10,0)).pack(side=tk.LEFT, padx=(10, 6))
        self.client_secret_var = tk.StringVar(value=self.config_data.get("client_secret", ""))
        ttk.Entry(top, textvariable=self.client_secret_var, width=40, show="•").pack(side=tk.LEFT)

        ttk.Button(top, text="Save", command=self._save_credentials).pack(side=tk.LEFT, padx=(10, 0))

        # Username row
        user_row = ttk.Frame(self)
        user_row.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(0, 10))
        ttk.Label(user_row, text="Spotify username:").pack(side=tk.LEFT)
        self.username_var = tk.StringVar(value=self.config_data.get("last_username", ""))
        ttk.Entry(user_row, textvariable=self.username_var, width=30).pack(side=tk.LEFT, padx=(6, 10))
        ttk.Button(user_row, text="OK", command=self._save_username).pack(side=tk.LEFT)

        # Progress bar
        prog = ttk.Frame(self)
        prog.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(0, 0))
        self.progress = ttk.Progressbar(prog, mode="determinate")
        self.progress.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.status_var = tk.StringVar(value="Idle")
        ttk.Label(prog, textvariable=self.status_var, width=36).pack(side=tk.LEFT, padx=(10, 0))

        # Action buttons
        actions = ttk.Frame(self)
        actions.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(10, 0))
        ttk.Button(actions, text="Extract", command=self._on_extract).pack(side=tk.LEFT)
        ttk.Button(actions, text="Remove Duplicates", command=self._remove_duplicates).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions, text="Export to Excel", command=self._export_excel).pack(side=tk.LEFT, padx=(8, 0))

        # Table
        table_container = ttk.Frame(self)
        table_container.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.tree = ttk.Treeview(table_container, columns=("artist", "title", "album", "playlist"), show="headings")
        self.tree.heading("artist", text="Artist", command=lambda: self._sort_table("artist"))
        self.tree.heading("title", text="Title", command=lambda: self._sort_table("title"))
        self.tree.heading("album", text="Album", command=lambda: self._sort_table("album"))
        self.tree.heading("playlist", text="Playlist Name", command=lambda: self._sort_table("playlist"))

        self.tree.column("artist", width=240, anchor="w")
        self.tree.column("title", width=260, anchor="w")
        self.tree.column("album", width=260, anchor="w")
        self.tree.column("playlist", width=260, anchor="w")

        vs = ttk.Scrollbar(table_container, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vs.set)
        vs.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Bottom bar: track count
        bottom = ttk.Frame(self)
        bottom.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=(0, 10))
        self.track_count_var = tk.StringVar(value="Tracks: 0")
        ttk.Label(bottom, textvariable=self.track_count_var, font=("TkDefaultFont", 10)).pack(side=tk.RIGHT)

        # Sort state
        self.sort_column = "album"
        self.sort_reverse = False

        # Poll queue
        self.after(100, self._poll_queue)

    # ----------------- Config -----------------

    def _load_config(self) -> Dict[str, Any]:
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_config(self):
        data = {
            "client_id": self.client_id_var.get().strip(),
            "client_secret": self.client_secret_var.get().strip(),
            "last_username": self.username_var.get().strip()
        }
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save config:\\n{e}")

    def _save_credentials(self):
        self._save_config()
        messagebox.showinfo("Saved", "Client ID and Secret saved (plaintext).")

    def _save_username(self):
        self._save_config()
        messagebox.showinfo("Saved", "Username saved.")

    # ----------------- Actions -----------------

    def _on_extract(self):
        client_id = self.client_id_var.get().strip()
        client_secret = self.client_secret_var.get().strip()
        user_id = self.username_var.get().strip()

        if not client_id or not client_secret or not user_id:
            messagebox.showwarning("Missing info", "Please enter Client ID, Client Secret, and Spotify username.")
            return

        # Reset table & progress
        self._clear_table()
        self.progress.configure(value=0, maximum=100)
        self.status_var.set("Authenticating…")

        # Save config
        self._save_config()

        # Start worker
        self.worker = SpotifyWorker(user_id, client_id, client_secret, self.q)
        self.worker.start()

    def _export_excel(self):
        if not self.rows:
            messagebox.showinfo("No data", "There is nothing to export yet.")
            return
        path = filedialog.asksaveasfilename(
            title="Export to Excel",
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")]
        )
        if not path:
            return
        try:
            import pandas as pd
            df = pd.DataFrame([
                {"Artist": r.artist, "Title": r.title, "Album": r.album, "Playlist Name": r.playlist}
                for r in self.rows
            ])
            # Keep current sort order in export
            df.to_excel(path, index=False)
            messagebox.showinfo("Exported", f"Exported {len(self.rows)} rows to:\\n{path}")
        except Exception as e:
            messagebox.showerror("Export error", f"Failed to export:\\n{e}")

    def _remove_duplicates(self):
        if not self.rows:
            messagebox.showinfo("No data", "There is no data to process.")
            return

        from collections import defaultdict

        # Group tracks by (artist, title) - case insensitive
        duplicates_map = defaultdict(list)
        for idx, row in enumerate(self.rows):
            key = (row.artist.lower(), row.title.lower())
            duplicates_map[key].append(idx)

        # Find which tracks to remove
        removed_tracks = []
        indices_to_remove = set()

        for key, indices in duplicates_map.items():
            if len(indices) <= 1:
                continue  # Not a duplicate

            # Count occurrences per playlist
            playlist_counts = defaultdict(list)
            for idx in indices:
                playlist_counts[self.rows[idx].playlist].append(idx)

            # Find playlist with most occurrences
            max_playlist = max(playlist_counts.items(), key=lambda x: len(x[1]))
            playlist_to_keep = max_playlist[0]
            indices_in_max_playlist = max_playlist[1]

            # Keep one from the playlist with most occurrences, remove all others
            keep_idx = indices_in_max_playlist[0]
            
            for idx in indices:
                if idx != keep_idx:
                    indices_to_remove.add(idx)
                    removed_tracks.append(self.rows[idx])

        # Remove duplicates
        if not removed_tracks:
            messagebox.showinfo("No Duplicates", "No duplicate tracks were found.")
            return

        # Create new rows list without duplicates
        new_rows = [row for idx, row in enumerate(self.rows) if idx not in indices_to_remove]
        
        # Update table
        self._clear_table()
        for r in new_rows:
            self.tree.insert("", tk.END, values=(r.artist, r.title, r.album, r.playlist))
        self.rows = new_rows
        self.track_count_var.set(f"Tracks: {len(new_rows)}")

        # Show results dialog
        self._show_duplicates_result(len(removed_tracks), removed_tracks)

    def _show_duplicates_result(self, count: int, removed_tracks: List[TrackRow]):
        """Show a dialog with duplicate removal results and an expandable list."""
        dialog = tk.Toplevel(self)
        dialog.title("Duplicates Removed")
        dialog.geometry("600x400")
        dialog.configure(bg='#2b2b2b')
        
        # Make it modal
        dialog.transient(self)
        dialog.grab_set()

        # Summary message
        summary_frame = ttk.Frame(dialog)
        summary_frame.pack(side=tk.TOP, fill=tk.X, padx=20, pady=20)
        
        summary_label = ttk.Label(
            summary_frame, 
            text=f"Successfully removed {count} duplicate track{'s' if count != 1 else ''}!",
            font=("TkDefaultFont", 11, "bold")
        )
        summary_label.pack()

        # Expandable list section
        list_frame = ttk.Frame(dialog)
        list_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=20, pady=(0, 10))

        # Toggle button and list
        self._list_visible = False
        list_container = ttk.Frame(list_frame)
        
        def toggle_list():
            if self._list_visible:
                list_container.pack_forget()
                toggle_btn.configure(text="Show List ▼")
                self._list_visible = False
            else:
                list_container.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=(10, 0))
                toggle_btn.configure(text="Hide List ▲")
                self._list_visible = True

        toggle_btn = ttk.Button(list_frame, text="Show List ▼", command=toggle_list)
        toggle_btn.pack(side=tk.TOP, pady=(0, 0))

        # Create the list (Treeview)
        tree = ttk.Treeview(
            list_container, 
            columns=("artist", "title", "album", "playlist"), 
            show="headings",
            height=10
        )
        tree.heading("artist", text="Artist")
        tree.heading("title", text="Title")
        tree.heading("album", text="Album")
        tree.heading("playlist", text="Playlist")
        
        tree.column("artist", width=150, anchor="w")
        tree.column("title", width=150, anchor="w")
        tree.column("album", width=130, anchor="w")
        tree.column("playlist", width=130, anchor="w")

        # Add scrollbar
        scrollbar = ttk.Scrollbar(list_container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Populate with removed tracks
        for track in removed_tracks:
            tree.insert("", tk.END, values=(track.artist, track.title, track.album, track.playlist))

        # Close button
        close_btn = ttk.Button(dialog, text="Close", command=dialog.destroy)
        close_btn.pack(side=tk.BOTTOM, pady=(0, 20))

        # Center dialog on parent
        dialog.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() // 2) - (dialog.winfo_width() // 2)
        y = self.winfo_y() + (self.winfo_height() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

    # ----------------- Table helpers -----------------

    def _clear_table(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.rows = []
        self.track_count_var.set("Tracks: 0")

    def _fill_table(self, rows: List[TrackRow]):
        # Initially grouped by album → sort by album (then artist/title)
        rows.sort(key=lambda r: (r.album.lower(), r.artist.lower(), r.title.lower()))
        for r in rows:
            self.tree.insert("", tk.END, values=(r.artist, r.title, r.album, r.playlist))
        self.rows = rows
        self.sort_column = "album"
        self.sort_reverse = False
        self.track_count_var.set(f"Tracks: {len(rows)}")

    def _sort_table(self, column: str):
        if not self.rows:
            return
        # Toggle if same column
        if self.sort_column == column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = False

        keyfunc = {
            "artist": lambda r: r.artist.lower(),
            "title":  lambda r: r.title.lower(),
            "album":  lambda r: r.album.lower(),
            "playlist": lambda r: r.playlist.lower()
        }[column]

        self.rows.sort(key=keyfunc, reverse=self.sort_reverse)
        # Refresh tree
        for item in self.tree.get_children():
            self.tree.delete(item)
        for r in self.rows:
            self.tree.insert("", tk.END, values=(r.artist, r.title, r.album, r.playlist))

    # ----------------- Queue / progress -----------------

    def _poll_queue(self):
        try:
            while True:
                msg, payload = self.q.get_nowait()
                if msg == "progress_total":
                    self.progress.configure(value=0, maximum=int(payload))
                    self.status_var.set(f"Processing… 0/{int(payload)}")
                elif msg == "progress":
                    val = int(payload)
                    self.progress.configure(value=val)
                    self.status_var.set(f"Processing… {val}/{int(self.progress['maximum'])}")
                elif msg == "message":
                    self.status_var.set(str(payload))
                elif msg == "done":
                    rows: List[TrackRow] = payload
                    self.status_var.set(f"Parsed {len(rows)} tracks.")
                    self._fill_table(rows)
                    self.progress.configure(value=self.progress['maximum'])
                elif msg == "error":
                    self.status_var.set("Error")
                    messagebox.showerror("Error", str(payload))
                self.q.task_done()
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)


if __name__ == "__main__":
    app = SpotifetchrApp()
    app.mainloop()
