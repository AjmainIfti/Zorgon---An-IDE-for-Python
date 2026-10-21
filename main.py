from __future__ import annotations

import bisect
import builtins
import codecs
import keyword
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from typing import Callable, Optional

APP_NAME = "Zorgon IDE"



#states
IDLE, RUNNING, STOPPING = "Idle", "Running", "Stopping"
FINISHED, FAILED, TERMINATED = "Finished", "Failed", "Terminated"


MAX_CONSOLE_LINES = 5000        
MAX_CONSOLE_CHARS = 1_000_000   
QUEUE_SIZE = 256                
READ_CHUNK = 4096
POLL_MS = 25
POLL_BUDGET_S = 0.012           
STOP_GRACE_S = 2.0             
FULL_HIGHLIGHT_LINES = 3000     
HIGHLIGHT_MARGIN = 150
HIGHLIGHT_DELAY_MS = 150
MIN_FONT, MAX_FONT, DEFAULT_FONT = 7, 40, 12




def read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def write_text_file(path: str, text: str) -> None:
    path = os.path.abspath(path)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".zorgon-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        if os.path.exists(path):
            shutil.copymode(path, tmp)
        else:
            os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def python_executable() -> str:
    exe = sys.executable
    if os.name == "nt" and exe.lower().endswith("pythonw.exe"):
        candidate = exe[:-len("pythonw.exe")] + "python.exe"
        if os.path.exists(candidate):
            return candidate
    return exe



class ScriptRunner:
    def __init__(self, script_path: str, python: Optional[str] = None,
                 grace: float = STOP_GRACE_S) -> None:
        self.script_path = os.path.abspath(script_path)
        self.python = python or python_executable()
        self.grace = grace
        self.events: "queue.Queue[tuple]" = queue.Queue(maxsize=QUEUE_SIZE)
        self.process: Optional[subprocess.Popen] = None
        self._readers: list[threading.Thread] = []
        self._waiter: Optional[threading.Thread] = None
        self._abandon = threading.Event()   
        self._closed = threading.Event()    
        self._stop_requested = False
        self._lock = threading.Lock()

    def start(self) -> None:
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUNBUFFERED"] = "1"
        extra = {}
        if os.name == "nt":
            extra["creationflags"] = subprocess.CREATE_NO_WINDOW
        self.process = subprocess.Popen(
            [self.python, "-u", self.script_path],      
            cwd=os.path.dirname(self.script_path),      
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            shell=False,
            **extra,
        )
        for stream, kind in ((self.process.stdout, "out"), (self.process.stderr, "err")):
            thread = threading.Thread(target=self._read, args=(stream, kind),
                                      name=f"zorgon-{kind}", daemon=True)
            self._readers.append(thread)
            thread.start()
        self._waiter = threading.Thread(target=self._wait, name="zorgon-wait", daemon=True)
        self._waiter.start()

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def send_input(self, text: str) -> bool:
        if not self.is_running() or self.process.stdin is None:
            return False
        data = text.encode("utf-8")
        try:
            while data:
                written = self.process.stdin.write(data)
                data = data[written:]
            return True
        except (BrokenPipeError, OSError, ValueError):
            return False

    def close_stdin(self) -> None:
        if self.process is not None and self.process.stdin is not None:
            try:
                self.process.stdin.close()
            except OSError:
                pass

    def stop(self) -> bool:
        with self._lock:
            if not self.is_running():
                return False
            self._stop_requested = True
            try:
                self.process.terminate()
            except OSError:
                pass
        timer = threading.Timer(self.grace, self._kill)
        timer.daemon = True
        timer.start()
        return True

    def shutdown(self) -> None:
        self._closed.set()
        self._abandon.set()
        proc = self.process
        if proc is not None and proc.poll() is None:
            self._stop_requested = True
            try:
                proc.terminate()
                proc.wait(timeout=self.grace)
            except subprocess.TimeoutExpired:
                self._kill()
                proc.wait()
            except OSError:
                pass
        for thread in self._readers + ([self._waiter] if self._waiter else []):
            thread.join(timeout=1.0)

    def _kill(self) -> None:
        proc = self.process
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except OSError:
                pass

    def _put(self, item: tuple, final: bool = False) -> bool:
        while not self._closed.is_set() and (final or not self._abandon.is_set()):
            try:
                self.events.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    def _worker_failed(self, message: str) -> None:
        self._put(("worker_error", message), final=True)
        self._kill()    

    def _read(self, stream, kind: str) -> None:
        try:
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            while True:
                chunk = stream.read(READ_CHUNK)
                if not chunk:
                    break
                text = decoder.decode(chunk)
                if text and not self._put((kind, text)):
                    return
            tail = decoder.decode(b"", final=True)
            if tail:
                self._put((kind, tail))
        except Exception as exc:  
            if not self._abandon.is_set():
                self._worker_failed(f"{kind} reader crashed: {exc!r}")

    def _wait(self) -> None:
        code: Optional[int] = None
        try:
            code = self.process.wait()
            for thread in self._readers:
                thread.join(timeout=3.0)
            if any(t.is_alive() for t in self._readers):
                self._abandon.set()
        except Exception as exc:  # noqa: BLE001
            self._put(("worker_error", f"wait thread crashed: {exc!r}"), final=True)
        finally:
            if not any(t.is_alive() for t in self._readers):
                for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                    try:
                        if stream is not None:
                            stream.close()
                    except OSError:
                        pass
            self._put(("exit", code, self._stop_requested), final=True)



_KEYWORDS = frozenset(keyword.kwlist)
_BUILTINS = frozenset(n for n in dir(builtins) if n[0].islower())
_TOKEN_RE = re.compile(
    r"""
    (?P<mstring>[rRbBuUfF]{0,2}(?:'''[\s\S]*?(?:'''|\Z)|\"\"\"[\s\S]*?(?:\"\"\"|\Z)))
  | (?P<string>[rRbBuUfF]{0,2}(?:'[^'\\\n]*(?:\\.[^'\\\n]*)*'?|"[^"\\\n]*(?:\\.[^"\\\n]*)*"?))
  | (?P<comment>\#[^\n]*)
  | (?P<defn>\b(?:def|class)\b[ \t]+[A-Za-z_]\w*)
  | (?P<number>\b(?:0[xX][0-9a-fA-F_]+|0[bB][01_]+|0[oO][0-7_]+|\d[\d_]*(?:\.[\d_]*)?(?:[eE][+-]?\d+)?)\b)
  | (?P<word>[A-Za-z_]\w*)
    """,
    re.VERBOSE,
)
_HIGHLIGHT_TAGS = ("keyword", "builtin", "string", "comment", "number", "defname")


THEME = {
    "bg": "#1e1f22", "fg": "#d4d4d4", "panel": "#2b2d30", "gutter_fg": "#6b7078",
    "select": "#264f78", "cursor": "#ffffff", "border": "#3c3f41",
    "keyword": "#c586c0", "builtin": "#4ec9b0", "string": "#ce9178",
    "comment": "#6a9955", "number": "#b5cea8", "defname": "#dcdcaa",
    "out": "#d4d4d4", "err": "#f48771", "info": "#569cd6", "input": "#dcdcaa",
    "ok": "#6a9955",
}


class ZorgonIDE:

    def __init__(self, root: Optional[tk.Tk] = None, initial_file: Optional[str] = None) -> None:
        self.root = root or tk.Tk()
        self.file_path = ""
        self.runner: Optional[ScriptRunner] = None
        self.state = IDLE
        self._poll_job: Optional[str] = None
        self._highlight_job: Optional[str] = None
        self._line_count = 0
        self._console_chars = 0
        self._last_output_time = 0.0
        self._prompt_pending = False
        self._worker_error = False
        self._closing = False

        aqua = self.root.tk.call("tk", "windowingsystem") == "aqua"
        self._mod = "Command" if aqua else "Control"
        self._mod_label = "Cmd+" if aqua else "Ctrl+"

        self.root.geometry("1000x720")
        self.root.minsize(560, 380)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        self._make_fonts()
        self._build_ui()
        self._build_menus()
        self._bind_keys()
        self._load_text("", "")
        self._set_state(IDLE)
        if initial_file:
            self.open_file(initial_file)

    def _make_fonts(self) -> None:
        families = set(tkfont.families(self.root))
        family = next((f for f in ("Cascadia Mono", "Consolas", "DejaVu Sans Mono", "Menlo",
                                   "Liberation Mono", "Courier New") if f in families), None)
        if family is None:
            family = tkfont.nametofont("TkFixedFont").actual("family")
        self.font = tkfont.Font(family=family, size=DEFAULT_FONT)

    def _build_ui(self) -> None:
        t = THEME
        self.root.configure(bg=t["bg"])
        style = ttk.Style(self.root)
        style.theme_use("clam")
        style.configure("TFrame", background=t["panel"])
        style.configure("TLabel", background=t["panel"], foreground=t["fg"])
        style.configure("TButton", background=t["border"], foreground=t["fg"],
                        borderwidth=0, padding=(10, 3))
        style.map("TButton", background=[("active", "#4a4d50"), ("disabled", t["panel"])],
                  foreground=[("disabled", "#6b7078")])
        style.configure("TEntry", fieldbackground=t["bg"], foreground=t["fg"],
                        insertcolor=t["cursor"], bordercolor=t["border"])
        style.configure("Vertical.TScrollbar", background=t["border"], troughcolor=t["bg"],
                        bordercolor=t["bg"], arrowcolor=t["fg"])
        style.configure("Horizontal.TScrollbar", background=t["border"], troughcolor=t["bg"],
                        bordercolor=t["bg"], arrowcolor=t["fg"])

        self.root.rowconfigure(1, weight=1)
        self.root.columnconfigure(0, weight=1)



        #toolbar
        bar = ttk.Frame(self.root, padding=(6, 4))
        bar.grid(row=0, column=0, sticky="ew")
        self.btn_run = ttk.Button(bar, text="\u25B6 Run", command=self.run)
        self.btn_stop = ttk.Button(bar, text="\u25A0 Stop", command=self.stop)
        self.btn_run.pack(side="left", padx=(0, 4))
        self.btn_stop.pack(side="left", padx=(0, 14))
        for label, command in (("New", self.new_file), ("Open", self.open_file),
                               ("Save", self.save)):
            ttk.Button(bar, text=label, command=command).pack(side="left", padx=(0, 4))




        #vertical window
        self.paned = tk.PanedWindow(self.root, orient="vertical", sashwidth=5, bd=0,
                                    bg=t["border"], sashrelief="flat")
        self.paned.grid(row=1, column=0, sticky="nsew")

        self.editor_frame = tk.Frame(self.paned, bg=t["bg"])
        self.editor_frame.rowconfigure(0, weight=1)
        self.editor_frame.columnconfigure(1, weight=1)
        self.gutter = tk.Text(self.editor_frame, width=4, padx=6, pady=2, bd=0,
                              highlightthickness=0, bg=t["panel"], fg=t["gutter_fg"],
                              font=self.font, wrap="none", state="disabled",
                              takefocus=0, cursor="arrow")
        self.gutter.tag_configure("num", justify="right")
        self.editor = tk.Text(self.editor_frame, wrap="none", undo=True, maxundo=-1,
                              autoseparators=True, bd=0, highlightthickness=0,
                              padx=6, pady=2, bg=t["bg"], fg=t["fg"], font=self.font,
                              insertbackground=t["cursor"], selectbackground=t["select"],
                              selectforeground=t["fg"], inactiveselectbackground=t["select"])
        self.vsb = ttk.Scrollbar(self.editor_frame, orient="vertical", command=self._yview)
        self.hsb = ttk.Scrollbar(self.editor_frame, orient="horizontal", command=self.editor.xview)
        self.editor.configure(yscrollcommand=self._on_yscroll, xscrollcommand=self.hsb.set)
        self.gutter.grid(row=0, column=0, sticky="ns")
        self.editor.grid(row=0, column=1, sticky="nsew")
        self.vsb.grid(row=0, column=2, sticky="ns")
        self.hsb.grid(row=1, column=1, sticky="ew")
        self.paned.add(self.editor_frame, stretch="always", minsize=120)

        self.console_frame = tk.Frame(self.paned, bg=t["panel"])
        self.console_frame.rowconfigure(0, weight=1)
        self.console_frame.columnconfigure(0, weight=1)
        self.console = tk.Text(self.console_frame, height=10, wrap="char", bd=0,
                               highlightthickness=0, padx=6, pady=2, bg=t["bg"], fg=t["out"],
                               font=self.font, state="disabled", selectbackground=t["select"],
                               inactiveselectbackground=t["select"])
        csb = ttk.Scrollbar(self.console_frame, orient="vertical", command=self.console.yview)
        self.console.configure(yscrollcommand=csb.set)
        self.console.grid(row=0, column=0, sticky="nsew")
        csb.grid(row=0, column=1, sticky="ns")
        for tag in ("out", "err", "info", "input", "ok"):
            self.console.tag_configure(tag, foreground=t[tag])
        self.console.bind("<Button-1>", lambda e: self.console.focus_set())

        row = ttk.Frame(self.console_frame, padding=(6, 3))
        row.grid(row=1, column=0, columnspan=2, sticky="ew")
        row.columnconfigure(1, weight=1)
        ttk.Label(row, text="Input:").grid(row=0, column=0, padx=(0, 6))
        self.input_var = tk.StringVar()
        self.input_entry = ttk.Entry(row, textvariable=self.input_var)
        self.input_entry.grid(row=0, column=1, sticky="ew")
        self.input_entry.bind("<Return>", lambda e: self.send_input())
        self.btn_send = ttk.Button(row, text="Send", command=self.send_input)
        self.btn_eof = ttk.Button(row, text="EOF", command=self.send_eof)
        self.btn_clear = ttk.Button(row, text="Clear", command=self.clear_console)
        self.btn_send.grid(row=0, column=2, padx=(6, 0))
        self.btn_eof.grid(row=0, column=3, padx=(4, 0))
        self.btn_clear.grid(row=0, column=4, padx=(4, 0))
        self.paned.add(self.console_frame, stretch="never", minsize=80)



        #status bar
        status = ttk.Frame(self.root, padding=(8, 2))
        status.grid(row=2, column=0, sticky="ew")
        status.columnconfigure(0, weight=1)
        self.status_var = tk.StringVar()
        self.pos_var = tk.StringVar(value="Ln 1, Col 1")
        ttk.Label(status, textvariable=self.status_var).grid(row=0, column=0, sticky="w")
        ttk.Label(status, textvariable=self.pos_var).grid(row=0, column=1, sticky="e")



        #tags
        for tag in _HIGHLIGHT_TAGS:
            self.editor.tag_configure(tag, foreground=t[tag])
        self._apply_font()
        self._install_change_proxy()
        self.editor.bind("<<Change>>", self._on_change)
        self.editor.bind("<KeyRelease>", lambda e: self._update_position())
        self.editor.bind("<ButtonRelease-1>", lambda e: self._update_position())
        self.editor.bind("<Return>", self._auto_indent)
        self.editor.bind("<Configure>", lambda e: self._schedule_highlight())
        self.editor.focus_set()

    def _build_menus(self) -> None:
        m = self._mod_label
        bar = tk.Menu(self.root)

        file_menu = tk.Menu(bar, tearoff=0)
        file_menu.add_command(label="New", accelerator=m + "N", command=self.new_file)
        file_menu.add_command(label="Open...", accelerator=m + "O", command=self.open_file)
        file_menu.add_command(label="Save", accelerator=m + "S", command=self.save)
        file_menu.add_command(label="Save As...", accelerator=m + "Shift+S", command=self.save_as)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", accelerator=m + "Q", command=self.on_close)
        bar.add_cascade(label="File", menu=file_menu)

        edit = tk.Menu(bar, tearoff=0)
        edit.add_command(label="Undo", accelerator=m + "Z", command=self.undo)
        edit.add_command(label="Redo", accelerator=m + "Y", command=self.redo)
        edit.add_separator()
        edit.add_command(label="Cut", accelerator=m + "X", command=lambda: self._clip("Cut"))
        edit.add_command(label="Copy", accelerator=m + "C", command=lambda: self._clip("Copy"))
        edit.add_command(label="Paste", accelerator=m + "V", command=lambda: self._clip("Paste"))
        edit.add_separator()
        edit.add_command(label="Select All", accelerator=m + "A", command=self.select_all)
        bar.add_cascade(label="Edit", menu=edit)

        self.run_menu = tk.Menu(bar, tearoff=0)
        self.run_menu.add_command(label="Run Python File", accelerator="F5", command=self.run)
        self.run_menu.add_command(label="Stop Execution", accelerator="Shift+F5", command=self.stop)
        self.run_menu.add_separator()
        self.run_menu.add_command(label="Clear Output", command=self.clear_console)
        bar.add_cascade(label="Run", menu=self.run_menu)

        view = tk.Menu(bar, tearoff=0)
        view.add_command(label="Increase Font Size", accelerator=m + "+",
                         command=lambda: self.change_font_size(1))
        view.add_command(label="Decrease Font Size", accelerator=m + "-",
                         command=lambda: self.change_font_size(-1))
        view.add_separator()
        self.show_lines = tk.BooleanVar(value=True)
        self.show_console = tk.BooleanVar(value=True)
        view.add_checkbutton(label="Line Numbers", variable=self.show_lines,
                             command=self._toggle_line_numbers)
        view.add_checkbutton(label="Console", variable=self.show_console,
                             command=self._toggle_console)
        bar.add_cascade(label="View", menu=view)
        self.root.config(menu=bar)

    def _bind_keys(self) -> None:
        mod = self._mod

        def cmd(func: Callable[[], object]):
            def handler(event=None):
                func()
                return "break"  
            return handler

        table = {
            f"<{mod}-n>": self.new_file, f"<{mod}-o>": self.open_file,
            f"<{mod}-s>": self.save, f"<{mod}-S>": self.save_as,
            f"<{mod}-q>": self.on_close,
            "<F5>": self.run, "<Shift-F5>": self.stop,
            f"<{mod}-a>": self.select_all,
            f"<{mod}-z>": self.undo, f"<{mod}-y>": self.redo, f"<{mod}-Z>": self.redo,
            f"<{mod}-plus>": lambda: self.change_font_size(1),
            f"<{mod}-equal>": lambda: self.change_font_size(1),
            f"<{mod}-minus>": lambda: self.change_font_size(-1),
        }
        for sequence, func in table.items():
            handler = cmd(func)
            self.root.bind(sequence, handler)
            self.editor.bind(sequence, handler) 

        def wheel(event):
            self.change_font_size(1 if getattr(event, "delta", 0) > 0 or event.num == 4 else -1)
            return "break"
        self.editor.bind(f"<{mod}-MouseWheel>", wheel)
        self.editor.bind(f"<{mod}-Button-4>", wheel)
        self.editor.bind(f"<{mod}-Button-5>", wheel)

    # 
    def _install_change_proxy(self) -> None:
        widget = str(self.editor)
        original = widget + "_orig"
        tcl = self.root.tk
        tcl.call("rename", widget, original)

        def proxy(*args):
            result = tcl.call((original,) + args)
            if args[:1] in (("insert",), ("delete",), ("replace",)) or \
                    args[:2] in (("edit", "undo"), ("edit", "redo")):
                self.editor.event_generate("<<Change>>", when="tail")
            return result

        tcl.createcommand(widget, proxy)

    def _on_change(self, event=None) -> None:
        self._update_line_numbers()
        self._update_title()
        self._update_position()
        self._schedule_highlight()

    @property
    def is_dirty(self) -> bool:
        return bool(self.editor.edit_modified())

    def get_text(self) -> str:
        return self.editor.get("1.0", "end-1c")   

    def _on_yscroll(self, first: str, last: str) -> None:
        self.vsb.set(first, last)
        self.gutter.yview_moveto(first)

    def _yview(self, *args) -> None:
        self.editor.yview(*args)

    def _update_line_numbers(self) -> None:
        count = int(self.editor.index("end-1c").split(".")[0])
        if count != self._line_count:
            self._line_count = count
            self.gutter.configure(state="normal", width=max(3, len(str(count))) + 1)
            self.gutter.delete("1.0", "end")
            self.gutter.insert("1.0", "\n".join(str(i) for i in range(1, count + 1)), "num")
            self.gutter.configure(state="disabled")
        self.gutter.yview_moveto(self.editor.yview()[0])

    def _update_position(self) -> None:
        line, col = self.editor.index("insert").split(".")
        self.pos_var.set(f"Ln {line}, Col {int(col) + 1}")

    def _schedule_highlight(self) -> None:
        if self._closing:
            return
        if self._highlight_job is not None:
            self.root.after_cancel(self._highlight_job)
        self._highlight_job = self.root.after(HIGHLIGHT_DELAY_MS, self._highlight)

    def _highlight(self) -> None:
        if self._highlight_job is not None:     
            self.root.after_cancel(self._highlight_job)
            self._highlight_job = None
        ed = self.editor
        total = int(ed.index("end-1c").split(".")[0])
        if total <= FULL_HIGHLIGHT_LINES:
            first, last = 1, total
        else:
            top = int(ed.index("@0,0").split(".")[0])
            bottom = int(ed.index(f"@0,{ed.winfo_height()}").split(".")[0])
            first, last = max(1, top - HIGHLIGHT_MARGIN), min(total, bottom + HIGHLIGHT_MARGIN)
        start, end = f"{first}.0", f"{last}.end"
        text = ed.get(start, end)
        for tag in _HIGHLIGHT_TAGS:
            ed.tag_remove(tag, start, end)

        starts = [0] + [m.end() for m in re.finditer("\n", text)]

        def pos(offset: int) -> str:
            line = bisect.bisect_right(starts, offset) - 1
            return f"{first + line}.{offset - starts[line]}"

        for m in _TOKEN_RE.finditer(text):
            kind = m.lastgroup
            if kind in ("mstring", "string"):
                ed.tag_add("string", pos(m.start()), pos(m.end()))
            elif kind == "comment":
                ed.tag_add("comment", pos(m.start()), pos(m.end()))
            elif kind == "number":
                ed.tag_add("number", pos(m.start()), pos(m.end()))
            elif kind == "defn":
                lead, name = m.group().split(None, 1)
                ed.tag_add("keyword", pos(m.start()), pos(m.start() + len(lead)))
                ed.tag_add("defname", pos(m.end() - len(name)), pos(m.end()))
            else:  # word
                word = m.group()
                if m.start() and text[m.start() - 1] == ".":
                    continue    
                if word in _KEYWORDS:
                    ed.tag_add("keyword", pos(m.start()), pos(m.end()))
                elif word in _BUILTINS:
                    ed.tag_add("builtin", pos(m.start()), pos(m.end()))

    def _auto_indent(self, event=None) -> str:
        ed = self.editor
        if ed.tag_ranges("sel"):
            ed.delete("sel.first", "sel.last")
        before = ed.get("insert linestart", "insert")
        indent = before[:len(before) - len(before.lstrip(" \t"))]
        if before.rstrip().endswith(":"):
            indent += "\t"
        ed.insert("insert", "\n" + indent)
        ed.see("insert")
        return "break"

    def _apply_font(self) -> None:
        self.editor.configure(tabs=(self.font.measure("    "),))   # tab = 4 columns

    def change_font_size(self, delta: int) -> None:
        size = max(MIN_FONT, min(MAX_FONT, int(self.font.cget("size")) + delta))
        self.font.configure(size=size)
        self._apply_font()

    def _toggle_line_numbers(self) -> None:
        if self.show_lines.get():
            self.gutter.grid()
        else:
            self.gutter.grid_remove()

    def _toggle_console(self) -> None:
        if self.show_console.get():
            self.paned.add(self.console_frame, stretch="never", minsize=80)
        else:
            self.paned.forget(self.console_frame)


    _FILETYPES = [("Python files", "*.py"), ("All files", "*.*")]

    def _ask_open_path(self) -> str:
        return filedialog.askopenfilename(parent=self.root, filetypes=self._FILETYPES) or ""

    def _ask_save_path(self) -> str:
        initial = os.path.basename(self.file_path) if self.file_path else "untitled.py"
        folder = os.path.dirname(self.file_path) if self.file_path else None
        return filedialog.asksaveasfilename(
            parent=self.root, defaultextension=".py", filetypes=self._FILETYPES,
            initialfile=initial, initialdir=folder) or ""

    def _ask_save_changes(self, name: str) -> Optional[bool]:
        return messagebox.askyesnocancel(
            APP_NAME, f"Save changes to {name}?", icon="warning", parent=self.root)

    def _ask_run_unsaved(self) -> Optional[bool]:
        return messagebox.askyesnocancel(
            APP_NAME,
            "This file has unsaved changes.\n\n"
            "Yes: save, then run\nNo: run the last saved version\nCancel: don't run",
            icon="warning", parent=self.root)

    def _show_error(self, title: str, message: str) -> None:
        messagebox.showerror(title, message, parent=self.root)

   
    @property
    def display_name(self) -> str:
        return os.path.basename(self.file_path) if self.file_path else "Untitled"

    def _update_title(self) -> None:
        mark = " *" if self.is_dirty else ""
        self.root.title(f"{APP_NAME} \u2014 {self.display_name}{mark}")

    def _load_text(self, text: str, path: str) -> None:
        self.editor.delete("1.0", "end")
        self.editor.insert("1.0", text)
        self.editor.edit_reset()            
        self.editor.edit_modified(False)
        self.editor.mark_set("insert", "1.0")
        self.editor.see("1.0")
        self.file_path = path
        self._on_change()

    def _confirm_discard(self) -> bool:
        
        if not self.is_dirty:
            return True
        answer = self._ask_save_changes(self.display_name)
        if answer is None:
            return False
        if answer:
            return self.save()      
        return True

   
    def new_file(self) -> bool:
        if not self._confirm_discard():
            return False
        self._load_text("", "")
        return True

    def open_file(self, path: Optional[str] = None) -> bool:
        if not path:
            path = self._ask_open_path()
        if not path:
            return False        
        try:
            text = read_text_file(path)
        except FileNotFoundError:
            self._show_error("Open failed", f"The file does not exist:\n{path}")
            return False
        except PermissionError:
            self._show_error("Open failed", f"No permission to read:\n{path}")
            return False
        except UnicodeDecodeError:
            self._show_error("Open failed", f"{path}\n\nis not a UTF-8 text file.")
            return False
        except OSError as exc:
            self._show_error("Open failed", f"Could not open {path}:\n{exc.strerror or exc}")
            return False
        if not self._confirm_discard():
            return False
        self._load_text(text, os.path.abspath(path))
        return True

    def save(self) -> bool:
        if not self.file_path:
            return self.save_as()
        return self._write_to(self.file_path)

    def save_as(self) -> bool:
        path = self._ask_save_path()
        if not path:
            return False        
        return self._write_to(path)

    def _write_to(self, path: str) -> bool:
        try:
            write_text_file(path, self.get_text())
        except OSError as exc:
            self._show_error("Save failed", f"Could not save {path}:\n{exc.strerror or exc}")
            return False
        self.file_path = os.path.abspath(path)      
        self.editor.edit_modified(False)
        self._update_title()
        return True

    def on_close(self) -> None:
        if not self._confirm_discard():
            return
        self._shutdown()
        self.root.destroy()

    def _shutdown(self) -> None:
        self._closing = True    
        for job in (self._poll_job, self._highlight_job):
            if job is not None:
                self.root.after_cancel(job)
        self._poll_job = self._highlight_job = None
        if self.runner is not None:
            self.runner.shutdown()      
            self.runner = None

    def undo(self) -> None:
        try:
            self.editor.edit_undo()
        except tk.TclError:
            pass    

    def redo(self) -> None:
        try:
            self.editor.edit_redo()
        except tk.TclError:
            pass    

    def _clip(self, which: str) -> None:
        widget = self.root.focus_get() or self.editor
        widget.event_generate(f"<<{which}>>")

    def select_all(self) -> None:
        widget = self.root.focus_get()
        if isinstance(widget, tk.Text):
            widget.tag_add("sel", "1.0", "end-1c")
        elif isinstance(widget, ttk.Entry):
            widget.select_range(0, "end")
        else:
            self.editor.tag_add("sel", "1.0", "end-1c")

    def is_running(self) -> bool:
        return self.runner is not None

    def _set_state(self, state: str, detail: str = "") -> None:
        self.state = state
        self.status_var.set(f"{state} \u2014 {detail}" if detail else state)
        running = state in (RUNNING, STOPPING)
        can_stop = state == RUNNING
        self.btn_run.configure(state="disabled" if running else "normal")
        self.btn_stop.configure(state="normal" if can_stop else "disabled")
        self.run_menu.entryconfigure(0, state="disabled" if running else "normal")
        self.run_menu.entryconfigure(1, state="normal" if can_stop else "disabled")
        io_state = "normal" if can_stop else "disabled"
        self.input_entry.configure(state=io_state)
        self.btn_send.configure(state=io_state)
        self.btn_eof.configure(state=io_state)

    def run(self) -> bool:
        if self.is_running():
            return False
        if not self.file_path:
            self._set_state(self.state, "save the file to run it")
            if not self.save_as():
                return False
        elif self.is_dirty:
            choice = self._ask_run_unsaved()
            if choice is None:
                return False
            if choice and not self.save():
                return False
        return self._start_run()

    def _start_run(self) -> bool:
        runner = ScriptRunner(self.file_path)
        self.clear_console()
        try:
            runner.start()
        except OSError as exc:
            self._console_write(f"[Could not start {runner.python}: {exc}]\n", "err")
            self._set_state(FAILED, "could not start the interpreter")
            self._show_error("Run failed", f"Could not start Python:\n{runner.python}\n\n{exc}")
            return False
        self.runner = runner
        self._worker_error = False
        self._prompt_pending = False
        self._console_write(f"$ {os.path.basename(runner.python)} {self.file_path}\n", "info")
        self._set_state(RUNNING)
        self.input_entry.focus_set()
        self._poll_job = self.root.after(POLL_MS, self._poll)
        return True

    def stop(self) -> bool:
        if self.runner is None:
            self.status_var.set(f"{self.state} \u2014 no script is running")
            return False
        if self.runner.stop():
            self._set_state(STOPPING, "waiting for the process to end")
            return True
        return False    # already exiting; the exit event is on its way

    def send_input(self) -> None:
        runner = self.runner
        if runner is None:
            return
        text = self.input_var.get()
        if runner.send_input(text + "\n"):
            self._console_write(text + "\n", "input")
            self.input_var.set("")
            self._prompt_pending = False
            self._set_state(RUNNING)
        else:
            self._console_write("[input not delivered: the program is not reading stdin]\n", "err")

    def send_eof(self) -> None:
        if self.runner is not None:
            self.runner.close_stdin()
            self._console_write("[EOF sent]\n", "info")

    def _poll(self) -> None:
        self._poll_job = None
        runner = self.runner
        if runner is None:
            return
        deadline = time.monotonic() + POLL_BUDGET_S
        kind: Optional[str] = None
        buffer: list[str] = []
        exit_event: Optional[tuple] = None

        def flush() -> None:
            nonlocal buffer
            if buffer:
                text = "".join(buffer)
                self._console_write(text, kind or "out")
                self._last_output_time = time.monotonic()
                if kind == "out":
                    self._prompt_pending = not text.endswith("\n")
                buffer = []

        while time.monotonic() < deadline:
            try:
                event = runner.events.get_nowait()
            except queue.Empty:
                break
            if event[0] in ("out", "err"):
                if event[0] != kind:
                    flush()
                    kind = event[0]
                buffer.append(event[1])
            elif event[0] == "worker_error":
                flush()
                self._worker_error = True
                self._console_write(f"[IDE error: {event[1]}]\n", "err")
            elif event[0] == "exit":
                exit_event = event
                break
        flush()

        if exit_event is not None:
            self._finish_run(exit_event)
            return
        if (self.state == RUNNING and self._prompt_pending
                and time.monotonic() - self._last_output_time > 0.4):
            self._set_state(RUNNING, "waiting for input?")
        self._poll_job = self.root.after(POLL_MS, self._poll)

    def _finish_run(self, event: tuple) -> None:
        _, code, stopped = event
        self.runner = None
        if stopped:
            self._console_write(f"[Process terminated by user (exit code {code})]\n", "info")
            self._set_state(TERMINATED, f"exit code {code}")
        elif code == 0 and not self._worker_error:
            self._console_write("[Process finished with exit code 0]\n", "ok")
            self._set_state(FINISHED, "exit code 0")
        else:
            shown = "unknown" if code is None else str(code)
            self._console_write(f"[Process failed with exit code {shown}]\n", "err")
            self._set_state(FAILED, f"exit code {shown}")
        self.editor.focus_set()

   
    def clear_console(self) -> None:
        self.console.configure(state="normal")
        self.console.delete("1.0", "end")
        self.console.configure(state="disabled")
        self._console_chars = 0

    def _console_write(self, text: str, tag: str = "out") -> None:
        if not text:
            return
        con = self.console
        con.configure(state="normal")
        last = con.get("end-2c", "end-1c")
        if tag in ("info", "ok") and last and last != "\n":
            text = "\n" + text      
        con.insert("end", text, tag)
        self._console_chars += len(text)
        self._trim_console()
        con.see("end")
        con.configure(state="disabled")

    def _trim_console(self) -> None:
        con = self.console
        lines = int(con.index("end-1c").split(".")[0])
        trimmed = False
        if lines > MAX_CONSOLE_LINES:
            con.delete("1.0", f"{lines - MAX_CONSOLE_LINES + 2}.0")
            trimmed = True
        if self._console_chars > MAX_CONSOLE_CHARS or trimmed:
            self._console_chars = int(con.count("1.0", "end-1c", "chars")[0])
            if self._console_chars > MAX_CONSOLE_CHARS:
                con.delete("1.0", f"1.0 + {self._console_chars - MAX_CONSOLE_CHARS} chars")
                self._console_chars = MAX_CONSOLE_CHARS
                trimmed = True
        if trimmed:
            con.insert("1.0", "[earlier output was trimmed]\n", "info")


def main(argv: Optional[list[str]] = None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    if os.name == "nt":
        try:    
            import ctypes
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except (AttributeError, OSError):
            pass
    ide = ZorgonIDE(initial_file=argv[0] if argv else None)
    ide.root.mainloop()


if __name__ == "__main__":
    main()