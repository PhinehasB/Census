"""
Tkinter OOP UI (Python 3.10+) for Oracle-backed Census DB
- Depends on: oracledb (pip install oracledb)
- Requires Oracle Instant Client or full client on PATH/LD_LIBRARY_PATH.
- Uses stored procedures & ref cursors defined in oracle_backend_objects.sql

Buttons:
  [Insert]   -> calls insert_person SP
  [Retrieve] -> calls get_person_by_id (JOIN returning details)
  [Update]   -> calls update_person SP
  [Delete]   -> calls delete_person SP

Also includes a simple search by locality (JOIN) and an activity log viewer.
"""
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, messagebox
from dataclasses import dataclass
import datetime as dt

try:
    import oracledb  # type: ignore
except Exception as e:
    oracledb = None

# ------------------------------
# Configuration (EDIT THESE)
# ------------------------------
ORACLE_HOST = "localhost"
ORACLE_PORT = 1521
ORACLE_SERVICE = ""   # e.g., ORCLPDB1 / FREEPDB1
ORACLE_USER = "C##Phine"
ORACLE_PASSWORD = "1234"

# ------------------------------
# Data Models
# ------------------------------
@dataclass
class Person:
    person_id: int | None = None
    ea_code: str | None = None
    structure_no: str | None = None
    household_no: str | None = None
    line_no: int | None = None
    full_name: str | None = None
    sex: str | None = None  # 'M' or 'F'
    date_of_birth: dt.date | None = None
    age_years: int | None = None
    nationality: str | None = None
    ethnicity: str | None = None
    religion: str | None = None
    marital_status: str | None = None

# ------------------------------
# DB LAYER
# ------------------------------
class OracleDB:
    def __init__(self, host: str, port: int, service: str, user: str, password: str):
        if oracledb is None:
            raise RuntimeError("oracledb not installed. Run: pip install oracledb")
        dsn = oracledb.makedsn(host, port, service_name=service)
        self.conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self.conn.autocommit = False

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    # Stored procedure: insert_person -> returns new person_id
    def sp_insert_person(self, p: Person) -> int:
        cur = self.conn.cursor()
        out_person_id = cur.var(oracledb.NUMBER)
        cur.callproc(
            "INSERT_PERSON",
            [p.ea_code, p.structure_no, p.household_no, p.line_no, p.full_name,
             p.sex, p.date_of_birth, p.age_years, p.nationality, p.ethnicity,
             p.religion, p.marital_status, out_person_id]
        )
        self.conn.commit()
        return int(out_person_id.getvalue())

    # Stored procedure: update_person
    def sp_update_person(self, p: Person) -> None:
        cur = self.conn.cursor()
        cur.callproc(
            "UPDATE_PERSON",
            [p.person_id, p.full_name, p.sex, p.date_of_birth, p.age_years,
             p.nationality, p.ethnicity, p.religion, p.marital_status]
        )
        self.conn.commit()

    # Stored procedure: delete_person
    def sp_delete_person(self, person_id: int) -> None:
        cur = self.conn.cursor()
        cur.callproc("DELETE_PERSON", [person_id])
        self.conn.commit()

    # Stored procedure returning ref cursor: get_person_by_id
    def sp_get_person_by_id(self, person_id: int) -> dict | None:
        cur = self.conn.cursor()
        out_cursor = cur.var(oracledb.CURSOR)
        cur.callproc("GET_PERSON_BY_ID", [person_id, out_cursor])
        rows = out_cursor.getvalue().fetchall()
        cols = [d[0].lower() for d in out_cursor.getvalue().description] if rows else []
        if not rows:
            return None
        # Single row expected
        row = rows[0]
        return {cols[i]: row[i] for i in range(len(cols))}

    # Stored procedure returning ref cursor: search by locality
    def sp_search_persons_by_locality(self, locality_code: str) -> list[dict]:
        cur = self.conn.cursor()
        out_cursor = cur.var(oracledb.CURSOR)
        cur.callproc("SEARCH_PERSONS_BY_LOCALITY", [locality_code, out_cursor])
        c = out_cursor.getvalue()
        cols = [d[0].lower() for d in c.description]
        return [ {cols[i]: r[i] for i in range(len(cols))} for r in c ]

    def fetch_activity_log(self, limit: int = 100) -> list[dict]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT log_id, table_name, action, key_value, username_, log_time
              FROM activity_log
             ORDER BY log_id DESC FETCH FIRST :lim ROWS ONLY
            """,
            {"lim": limit}
        )
        cols = [d[0].lower() for d in cur.description]
        return [ {cols[i]: r[i] for i in range(len(cols))} for r in cur ]

# ------------------------------
# UI LAYER
# ------------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Census CRUD (Oracle)")
        self.geometry("920x640")
        self.resizable(True, True)
        self.db: OracleDB | None = None
        self._init_widgets()

    # UI building
    def _init_widgets(self):
        frm_cfg = ttk.LabelFrame(self, text="Connection")
        frm_cfg.pack(fill="x", padx=10, pady=8)

        self.user_var = tk.StringVar(value=ORACLE_USER)
        self.pwd_var = tk.StringVar(value=ORACLE_PASSWORD)
        self.host_var = tk.StringVar(value=ORACLE_HOST)
        self.port_var = tk.IntVar(value=ORACLE_PORT)
        self.svc_var  = tk.StringVar(value=ORACLE_SERVICE)

        def add_labeled(parent, text, var, width=18):
            lbl = ttk.Label(parent, text=text)
            ent = ttk.Entry(parent, textvariable=var, width=width)
            return lbl, ent

        grid = ttk.Frame(frm_cfg)
        grid.pack(fill="x", padx=10, pady=6)
        items = [
            ("User", self.user_var), ("Password", self.pwd_var),
            ("Host", self.host_var), ("Port", self.port_var), ("Service", self.svc_var)
        ]
        for i, (label, var) in enumerate(items):
            lbl = ttk.Label(grid, text=label)
            lbl.grid(row=0, column=i*2, sticky="w", padx=4)
            ent = ttk.Entry(grid, textvariable=var, width=16, show="*" if label=="Password" else None)
            ent.grid(row=0, column=i*2+1, padx=4)

        btn_conn = ttk.Button(frm_cfg, text="Connect", command=self.connect_db)
        btn_conn.pack(padx=10, pady=6)

        frm_form = ttk.LabelFrame(self, text="Person")
        frm_form.pack(fill="x", padx=10, pady=8)

        # Person fields
        self.vars = {
            'person_id': tk.StringVar(),
            'ea_code': tk.StringVar(),
            'structure_no': tk.StringVar(),
            'household_no': tk.StringVar(),
            'line_no': tk.StringVar(),
            'full_name': tk.StringVar(),
            'sex': tk.StringVar(),
            'dob': tk.StringVar(),
            'age_years': tk.StringVar(),
            'nationality': tk.StringVar(),
            'ethnicity': tk.StringVar(),
            'religion': tk.StringVar(),
            'marital_status': tk.StringVar(),
            'locality_code': tk.StringVar(),  # for search
        }

        labels = [
            ("Person ID", 'person_id'), ("EA Code", 'ea_code'), ("Structure No", 'structure_no'),
            ("Household No", 'household_no'), ("Line No", 'line_no'), ("Full Name", 'full_name'),
            ("Sex (M/F)", 'sex'), ("DOB (YYYY-MM-DD)", 'dob'), ("Age Years", 'age_years'),
            ("Nationality", 'nationality'), ("Ethnicity", 'ethnicity'), ("Religion", 'religion'),
            ("Marital Status", 'marital_status'), ("Search Locality Code", 'locality_code')
        ]
        form_grid = ttk.Frame(frm_form)
        form_grid.pack(fill="x", padx=10, pady=6)
        for i, (label, key) in enumerate(labels):
            r, c = divmod(i, 4)
            ttk.Label(form_grid, text=label).grid(row=r, column=c*2, sticky="w", padx=4, pady=3)
            ttk.Entry(form_grid, textvariable=self.vars[key], width=20).grid(row=r, column=c*2+1, padx=4, pady=3)

        # Buttons
        frm_btns = ttk.Frame(self)
        frm_btns.pack(fill="x", padx=10, pady=6)
        ttk.Button(frm_btns, text="Insert", command=self.on_insert).pack(side="left", padx=4)
        ttk.Button(frm_btns, text="Retrieve", command=self.on_retrieve).pack(side="left", padx=4)
        ttk.Button(frm_btns, text="Update", command=self.on_update).pack(side="left", padx=4)
        ttk.Button(frm_btns, text="Delete", command=self.on_delete).pack(side="left", padx=4)
        ttk.Button(frm_btns, text="Search by Locality", command=self.on_search_locality).pack(side="left", padx=12)
        ttk.Button(frm_btns, text="View Activity Log", command=self.on_view_log).pack(side="left", padx=4)

        # Results area
        self.txt = tk.Text(self, height=16)
        self.txt.pack(fill="both", expand=True, padx=10, pady=8)
        self._log("Ready. Enter connection details and click Connect.")

    def _log(self, msg: str):
        self.txt.insert("end", f"{msg}\n")
        self.txt.see("end")

    def connect_db(self):
        try:
            self.db = OracleDB(
                self.host_var.get(), int(self.port_var.get()), self.svc_var.get(),
                self.user_var.get(), self.pwd_var.get()
            )
            messagebox.showinfo("DB", "Connected to Oracle.")
            self._log("Connected to Oracle.")
        except Exception as e:
            messagebox.showerror("Connection error", str(e))
            self._log(f"Connection failed: {e}")

    def _read_person_from_form(self) -> Person:
        def to_int(s: str | None):
            try:
                return int(s) if s not in (None, "") else None
            except ValueError:
                return None
        def to_date(s: str | None):
            try:
                return dt.datetime.strptime(s, "%Y-%m-%d").date() if s else None
            except ValueError:
                return None
        return Person(
            person_id=to_int(self.vars['person_id'].get()),
            ea_code=self.vars['ea_code'].get() or None,
            structure_no=self.vars['structure_no'].get() or None,
            household_no=self.vars['household_no'].get() or None,
            line_no=to_int(self.vars['line_no'].get()),
            full_name=self.vars['full_name'].get() or None,
            sex=(self.vars['sex'].get() or None),
            date_of_birth=to_date(self.vars['dob'].get()),
            age_years=to_int(self.vars['age_years'].get()),
            nationality=self.vars['nationality'].get() or None,
            ethnicity=self.vars['ethnicity'].get() or None,
            religion=self.vars['religion'].get() or None,
            marital_status=self.vars['marital_status'].get() or None,
        )

    # ------------------------------
    # Button handlers
    # ------------------------------
    def on_insert(self):
        if not self.db:
            return messagebox.showwarning("DB", "Not connected.")
        p = self._read_person_from_form()
        try:
            new_id = self.db.sp_insert_person(p)
            self.vars['person_id'].set(str(new_id))
            msg = f"Inserted person_id={new_id}"
            messagebox.showinfo("Insert", msg)
            self._log(msg)
        except Exception as e:
            messagebox.showerror("Insert failed", str(e))
            self._log(f"Insert failed: {e}")

    def on_retrieve(self):
        if not self.db:
            return messagebox.showwarning("DB", "Not connected.")
        pid = self.vars['person_id'].get()
        if not pid:
            return messagebox.showwarning("Retrieve", "Enter Person ID first.")
        try:
            data = self.db.sp_get_person_by_id(int(pid))
            if not data:
                self._log("No record found.")
                return
            # fill some fields
            self.vars['full_name'].set(data.get('full_name') or '')
            self.vars['sex'].set(data.get('sex') or '')
            self.vars['age_years'].set(str(data.get('age_years') or ''))
            self.vars['ea_code'].set(data.get('ea_code') or '')
            self.vars['structure_no'].set(data.get('structure_no') or '')
            self.vars['household_no'].set(data.get('household_no') or '')
            self.vars['locality_code'].set(data.get('locality_code') or '')
            self._log(f"Retrieved: {data}")
        except Exception as e:
            messagebox.showerror("Retrieve failed", str(e))
            self._log(f"Retrieve failed: {e}")

    def on_update(self):
        if not self.db:
            return messagebox.showwarning("DB", "Not connected.")
        p = self._read_person_from_form()
        if not p.person_id:
            return messagebox.showwarning("Update", "Enter Person ID to update.")
        try:
            self.db.sp_update_person(p)
            messagebox.showinfo("Update", "Updated successfully.")
            self._log(f"Updated person_id={p.person_id}")
        except Exception as e:
            messagebox.showerror("Update failed", str(e))
            self._log(f"Update failed: {e}")

    def on_delete(self):
        if not self.db:
            return messagebox.showwarning("DB", "Not connected.")
        pid = self.vars['person_id'].get()
        if not pid:
            return messagebox.showwarning("Delete", "Enter Person ID to delete.")
        if not messagebox.askyesno("Confirm", f"Delete person_id={pid}?"):
            return
        try:
            self.db.sp_delete_person(int(pid))
            messagebox.showinfo("Delete", "Deleted successfully.")
            self._log(f"Deleted person_id={pid}")
        except Exception as e:
            messagebox.showerror("Delete failed", str(e))
            self._log(f"Delete failed: {e}")

    def on_search_locality(self):
        if not self.db:
            return messagebox.showwarning("DB", "Not connected.")
        loc = self.vars['locality_code'].get()
        if not loc:
            return messagebox.showwarning("Search", "Enter Locality Code.")
        try:
            rows = self.db.sp_search_persons_by_locality(loc)
            self._log(f"Search results for locality={loc}: {len(rows)} row(s)")
            for r in rows:
                self._log(str(r))
        except Exception as e:
            messagebox.showerror("Search failed", str(e))
            self._log(f"Search failed: {e}")

    def on_view_log(self):
        if not self.db:
            return messagebox.showwarning("DB", "Not connected.")
        try:
            rows = self.db.fetch_activity_log(limit=50)
            self._log("Activity Log (latest 50):")
            for r in rows:
                self._log(f"#{r['log_id']} {r['log_time']} {r['table_name']} {r['action']} key={r['key_value']} by {r['username_']}")
        except Exception as e:
            messagebox.showerror("Log fetch failed", str(e))
            self._log(f"Log fetch failed: {e}")

if __name__ == "__main__":
    app = App()
    app.mainloop()