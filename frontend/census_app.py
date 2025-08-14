"""
Enhanced Tkinter Census Database Management System
- Comprehensive UI for Oracle Census DB with multiple modules
- Depends on: oracledb (pip install oracledb)
- Features: Geography management, household data, person records, reports, and more
"""
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from dataclasses import dataclass
import datetime as dt
from typing import Dict, List, Optional, Any
import csv

try:
    import oracledb  # type: ignore
except Exception as e:
    oracledb = None


# ------------------------------
# Configuration
# ------------------------------
ORACLE_HOST = "localhost"
ORACLE_PORT = 1521
ORACLE_SERVICE = "FREEPDB1"
ORACLE_USER = "C##Phine"
ORACLE_PASSWORD = "1234"

# ------------------------------
# Database Layer
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

    def execute_query(self, sql: str, params: dict = None) -> List[Dict]:
        """Execute a query and return results as list of dictionaries"""
        cur = self.conn.cursor()
        cur.execute(sql, params or {})
        cols = [d[0].lower() for d in cur.description] if cur.description else []
        return [{cols[i]: r[i] for i in range(len(cols))} for r in cur]

    def execute_non_query(self, sql: str, params: dict = None) -> int:
        """Execute insert/update/delete and return affected rows"""
        cur = self.conn.cursor()
        cur.execute(sql, params or {})
        affected = cur.rowcount
        self.conn.commit()
        return affected

    # Geography methods
    def get_regions(self) -> List[Dict]:
        return self.execute_query("SELECT * FROM region ORDER BY region_name")

    def get_districts(self, region_code: str = None) -> List[Dict]:
        if region_code:
            return self.execute_query(
                "SELECT * FROM district WHERE region_code = :reg ORDER BY district_name",
                {"reg": region_code}
            )
        return self.execute_query("SELECT * FROM district ORDER BY district_name")

    def get_localities(self, subdistrict_code: str = None) -> List[Dict]:
        if subdistrict_code:
            return self.execute_query(
                "SELECT * FROM locality WHERE subdistrict_code = :sub ORDER BY locality_name",
                {"sub": subdistrict_code}
            )
        return self.execute_query("SELECT * FROM locality ORDER BY locality_name")

    def get_enumeration_areas(self, locality_code: str = None) -> List[Dict]:
        if locality_code:
            return self.execute_query(
                "SELECT * FROM enumeration_area WHERE locality_code = :loc ORDER BY ea_code",
                {"loc": locality_code}
            )
        return self.execute_query("SELECT * FROM enumeration_area ORDER BY ea_code")

    # Household methods
    def get_households(self, ea_code: str = None) -> List[Dict]:
        if ea_code:
            return self.execute_query(
                """SELECT h.*, l.locality_name, ea.ea_number 
                   FROM household h 
                   LEFT JOIN locality l ON h.locality_code = l.locality_code
                   LEFT JOIN enumeration_area ea ON h.ea_code = ea.ea_code
                   WHERE h.ea_code = :ea ORDER BY h.structure_no, h.household_no""",
                {"ea": ea_code}
            )
        return self.execute_query(
            """SELECT h.*, l.locality_name, ea.ea_number 
               FROM household h 
               LEFT JOIN locality l ON h.locality_code = l.locality_code
               LEFT JOIN enumeration_area ea ON h.ea_code = ea.ea_code
               ORDER BY h.ea_code, h.structure_no, h.household_no FETCH FIRST 100 ROWS ONLY"""
        )

    def insert_household(self, data: Dict) -> int:
        sql = """INSERT INTO household (ea_code, structure_no, household_no, locality_code,
                 type_of_residence, address_detail, phone1, phone2)
                 VALUES (:ea_code, :structure_no, :household_no, :locality_code,
                 :type_of_residence, :address_detail, :phone1, :phone2)"""
        return self.execute_non_query(sql, data)

    # Person methods
    def get_persons(self, household_key: tuple = None, limit: int = 100) -> List[Dict]:
        if household_key:
            ea_code, structure_no, household_no = household_key
            return self.execute_query(
                """SELECT p.*, h.locality_code, l.locality_name 
                   FROM person p
                   LEFT JOIN household h ON (p.ea_code = h.ea_code AND p.structure_no = h.structure_no 
                                           AND p.household_no = h.household_no)
                   LEFT JOIN locality l ON h.locality_code = l.locality_code
                   WHERE p.ea_code = :ea AND p.structure_no = :struct AND p.household_no = :hh
                   ORDER BY p.line_no""",
                {"ea": ea_code, "struct": structure_no, "hh": household_no}
            )
        return self.execute_query(
            """SELECT p.*, h.locality_code, l.locality_name 
               FROM person p
               LEFT JOIN household h ON (p.ea_code = h.ea_code AND p.structure_no = h.structure_no 
                                       AND p.household_no = h.household_no)
               LEFT JOIN locality l ON h.locality_code = l.locality_code
               ORDER BY p.person_id DESC FETCH FIRST :lim ROWS ONLY""",
            {"lim": limit}
        )

    # Stored procedures (from original code)
    def sp_insert_person(self, data: Dict) -> int:
        cur = self.conn.cursor()
        out_person_id = cur.var(oracledb.NUMBER)
        cur.callproc(
            "INSERT_PERSON",
            [data.get('ea_code'), data.get('structure_no'), data.get('household_no'),
             data.get('line_no'), data.get('full_name'), data.get('sex'),
             data.get('date_of_birth'), data.get('age_years'), data.get('nationality'),
             data.get('ethnicity'), data.get('religion'), data.get('marital_status'),
             out_person_id]
        )
        self.conn.commit()
        return int(out_person_id.getvalue())

    def sp_get_person_by_id(self, person_id: int) -> dict | None:
        cur = self.conn.cursor()
        out_cursor = cur.var(oracledb.CURSOR)
        cur.callproc("GET_PERSON_BY_ID", [person_id, out_cursor])
        rows = out_cursor.getvalue().fetchall()
        cols = [d[0].lower() for d in out_cursor.getvalue().description] if rows else []
        if not rows:
            return None
        row = rows[0]
        return {cols[i]: row[i] for i in range(len(cols))}

    def get_activity_log(self, limit: int = 50) -> List[Dict]:
        return self.execute_query(
            """SELECT log_id, table_name, action, key_value, username_, log_time, details
               FROM activity_log ORDER BY log_id DESC FETCH FIRST :lim ROWS ONLY""",
            {"lim": limit}
        )

    # Reports and statistics
    def get_population_summary(self) -> List[Dict]:
        return self.execute_query(
            """SELECT r.region_name, d.district_name, COUNT(p.person_id) as population,
                      COUNT(CASE WHEN UPPER(p.sex) LIKE 'M%' THEN 1 END) as male_count,
                      COUNT(CASE WHEN UPPER(p.sex) LIKE 'F%' THEN 1 END) as female_count,
                      ROUND(AVG(p.age_years), 1) as avg_age
               FROM person p
               JOIN household h ON (p.ea_code = h.ea_code AND p.structure_no = h.structure_no 
                                  AND p.household_no = h.household_no)
               JOIN locality l ON h.locality_code = l.locality_code
               JOIN subdistrict sd ON l.subdistrict_code = sd.subdistrict_code
               JOIN district d ON sd.district_code = d.district_code
               JOIN region r ON d.region_code = r.region_code
               GROUP BY r.region_name, d.district_name
               ORDER BY r.region_name, d.district_name"""
        )

    def get_age_distribution(self) -> List[Dict]:
        return self.execute_query(
            """SELECT 
                 CASE 
                   WHEN age_years < 5 THEN '0-4'
                   WHEN age_years < 15 THEN '5-14' 
                   WHEN age_years < 25 THEN '15-24'
                   WHEN age_years < 35 THEN '25-34'
                   WHEN age_years < 45 THEN '35-44'
                   WHEN age_years < 55 THEN '45-54'
                   WHEN age_years < 65 THEN '55-64'
                   ELSE '65+'
                 END as age_group,
                 COUNT(*) as count,
                 ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
               FROM person 
               WHERE age_years IS NOT NULL
               GROUP BY CASE 
                   WHEN age_years < 5 THEN '0-4'
                   WHEN age_years < 15 THEN '5-14' 
                   WHEN age_years < 25 THEN '15-24'
                   WHEN age_years < 35 THEN '25-34'
                   WHEN age_years < 45 THEN '35-44'
                   WHEN age_years < 55 THEN '45-54'
                   WHEN age_years < 65 THEN '55-64'
                   ELSE '65+'
                 END
               ORDER BY MIN(age_years)"""
        )

# ------------------------------
# UI Components
# ------------------------------
class DataTreeview(ttk.Frame):
    """Reusable treeview component with search and export"""
    def __init__(self, parent, columns: List[str], **kwargs):
        super().__init__(parent, **kwargs)
        self.columns = columns
        self._init_widgets()

    def _init_widgets(self):
        # Search frame
        search_frame = ttk.Frame(self)
        search_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Label(search_frame, text="Search:").pack(side="left")
        self.search_var = tk.StringVar()
        self.search_var.trace('w', self._on_search)
        ttk.Entry(search_frame, textvariable=self.search_var, width=30).pack(side="left", padx=5)
        
        ttk.Button(search_frame, text="Export CSV", command=self._export_csv).pack(side="right", padx=5)
        ttk.Button(search_frame, text="Refresh", command=self.refresh).pack(side="right")

        # Treeview with scrollbars
        tree_frame = ttk.Frame(self)
        tree_frame.pack(fill="both", expand=True, padx=5, pady=5)

        self.tree = ttk.Treeview(tree_frame, columns=self.columns, show="headings", height=15)
        
        # Scrollbars
        v_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        h_scroll = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)

        # Pack treeview and scrollbars
        self.tree.grid(row=0, column=0, sticky="nsew")
        v_scroll.grid(row=0, column=1, sticky="ns")
        h_scroll.grid(row=1, column=0, sticky="ew")
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # Configure columns
        for col in self.columns:
            self.tree.heading(col, text=col.replace('_', ' ').title())
            self.tree.column(col, width=120, anchor="center")

        self.data = []

    def load_data(self, data: List[Dict]):
        """Load data into the treeview"""
        self.data = data
        self._refresh_tree()

    def _refresh_tree(self):
        """Refresh tree with current data and search filter"""
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)

        # Apply search filter
        search_term = self.search_var.get().lower()
        filtered_data = self.data
        
        if search_term:
            filtered_data = []
            for row in self.data:
                if any(search_term in str(row.get(col, '')).lower() for col in self.columns):
                    filtered_data.append(row)

        # Insert filtered data
        for row in filtered_data:
            values = [row.get(col, '') for col in self.columns]
            self.tree.insert("", "end", values=values)

    def _on_search(self, *args):
        """Handle search input"""
        self._refresh_tree()

    def _export_csv(self):
        """Export current data to CSV"""
        if not self.data:
            messagebox.showwarning("Export", "No data to export")
            return

        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=self.columns)
                    writer.writeheader()
                    for row in self.data:
                        filtered_row = {col: row.get(col, '') for col in self.columns}
                        writer.writerow(filtered_row)
                messagebox.showinfo("Export", f"Data exported to {filename}")
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export: {e}")

    def refresh(self):
        """Override this method to refresh data"""
        pass

    def get_selected_row(self) -> Dict | None:
        """Get the selected row data"""
        selection = self.tree.selection()
        if not selection:
            return None
        
        item = selection[0]
        values = self.tree.item(item, "values")
        return {self.columns[i]: values[i] for i in range(len(self.columns))}

# ------------------------------
# Main Application
# ------------------------------
class CensusApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Census Database Management System")
        self.geometry("1400x800")
        self.state('zoomed' if tk.TkVersion >= 8.5 else 'normal')
        
        self.db: Optional[OracleDB] = None
        self._init_styles()
        self._init_widgets()

    def _init_styles(self):
        """Configure ttk styles"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure custom styles
        style.configure('Header.TLabel', font=('Arial', 12, 'bold'))
        style.configure('Title.TLabel', font=('Arial', 14, 'bold'))

    def _init_widgets(self):
        # Main menu
        self._create_menu()
        
        # Connection frame
        self._create_connection_frame()
        
        # Main notebook (tabs)
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Create tabs
        self._create_dashboard_tab()
        self._create_geography_tab()
        self._create_household_tab()
        self._create_person_tab()
        self._create_reports_tab()
        self._create_admin_tab()

    def _create_menu(self):
        """Create the main menu bar"""
        menubar = tk.Menu(self)
        self.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Connect to Database", command=self.connect_db)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.quit)
        
        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="Refresh All Data", command=self.refresh_all_data)
        tools_menu.add_command(label="Database Statistics", command=self.show_db_stats)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)

    def _create_connection_frame(self):
        """Create database connection frame"""
        conn_frame = ttk.LabelFrame(self, text="Database Connection")
        conn_frame.pack(fill="x", padx=10, pady=5)
        
        # Connection fields
        fields_frame = ttk.Frame(conn_frame)
        fields_frame.pack(fill="x", padx=10, pady=5)
        
        self.host_var = tk.StringVar(value=ORACLE_HOST)
        self.port_var = tk.StringVar(value=str(ORACLE_PORT))
        self.service_var = tk.StringVar(value=ORACLE_SERVICE)
        self.user_var = tk.StringVar(value=ORACLE_USER)
        self.pwd_var = tk.StringVar(value=ORACLE_PASSWORD)
        
        fields = [
            ("Host:", self.host_var, 15),
            ("Port:", self.port_var, 8),
            ("Service:", self.service_var, 15),
            ("Username:", self.user_var, 15),
            ("Password:", self.pwd_var, 15)
        ]
        
        for i, (label, var, width) in enumerate(fields):
            ttk.Label(fields_frame, text=label).grid(row=0, column=i*2, sticky="w", padx=5)
            show_chars = None if label != "Password:" else "*"
            ttk.Entry(fields_frame, textvariable=var, width=width, show=show_chars).grid(
                row=0, column=i*2+1, padx=5
            )
        
        # Connection status and button
        status_frame = ttk.Frame(conn_frame)
        status_frame.pack(fill="x", padx=10, pady=5)
        
        self.conn_status = ttk.Label(status_frame, text="Not Connected", foreground="red")
        self.conn_status.pack(side="left")
        
        ttk.Button(status_frame, text="Connect", command=self.connect_db).pack(side="right", padx=5)
        ttk.Button(status_frame, text="Disconnect", command=self.disconnect_db).pack(side="right")

    def _create_dashboard_tab(self):
        """Create dashboard overview tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📊 Dashboard")
        
        # Statistics frame
        stats_frame = ttk.LabelFrame(tab, text="Quick Statistics")
        stats_frame.pack(fill="x", padx=10, pady=10)
        
        self.stats_text = tk.Text(stats_frame, height=8, wrap="word")
        self.stats_text.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Recent activity frame
        activity_frame = ttk.LabelFrame(tab, text="Recent Activity")
        activity_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.activity_tree = DataTreeview(
            activity_frame, 
            ["log_id", "table_name", "action", "key_value", "username_", "log_time"]
        )
        self.activity_tree.pack(fill="both", expand=True)

    def _create_geography_tab(self):
        """Create geography management tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🗺️ Geography")
        
        # Navigation frame
        nav_frame = ttk.LabelFrame(tab, text="Geographic Navigation")
        nav_frame.pack(fill="x", padx=10, pady=5)
        
        nav_grid = ttk.Frame(nav_frame)
        nav_grid.pack(fill="x", padx=10, pady=5)
        
        # Geographic selection widgets
        ttk.Label(nav_grid, text="Region:").grid(row=0, column=0, sticky="w", padx=5)
        self.region_var = tk.StringVar()
        self.region_combo = ttk.Combobox(nav_grid, textvariable=self.region_var, width=20)
        self.region_combo.grid(row=0, column=1, padx=5)
        self.region_combo.bind('<<ComboboxSelected>>', self._on_region_selected)
        
        ttk.Label(nav_grid, text="District:").grid(row=0, column=2, sticky="w", padx=5)
        self.district_var = tk.StringVar()
        self.district_combo = ttk.Combobox(nav_grid, textvariable=self.district_var, width=20)
        self.district_combo.grid(row=0, column=3, padx=5)
        
        # Enumeration Areas
        ea_frame = ttk.LabelFrame(tab, text="Enumeration Areas")
        ea_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.ea_tree = DataTreeview(
            ea_frame,
            ["ea_code", "ea_number", "locality_code", "ea_type", "region_code", "district_code"]
        )
        self.ea_tree.pack(fill="both", expand=True)

    def _create_household_tab(self):
        """Create household management tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🏠 Households")
        
        # Household form frame
        form_frame = ttk.LabelFrame(tab, text="Household Information")
        form_frame.pack(fill="x", padx=10, pady=5)
        
        # Household fields
        self.household_vars = {
            'ea_code': tk.StringVar(),
            'structure_no': tk.StringVar(),
            'household_no': tk.StringVar(),
            'locality_code': tk.StringVar(),
            'type_of_residence': tk.StringVar(),
            'address_detail': tk.StringVar(),
            'phone1': tk.StringVar(),
            'phone2': tk.StringVar()
        }
        
        form_grid = ttk.Frame(form_frame)
        form_grid.pack(fill="x", padx=10, pady=5)
        
        household_fields = [
            ("EA Code:", 'ea_code'), ("Structure No:", 'structure_no'),
            ("Household No:", 'household_no'), ("Locality Code:", 'locality_code'),
            ("Residence Type:", 'type_of_residence'), ("Address:", 'address_detail'),
            ("Phone 1:", 'phone1'), ("Phone 2:", 'phone2')
        ]
        
        for i, (label, key) in enumerate(household_fields):
            row, col = divmod(i, 4)
            ttk.Label(form_grid, text=label).grid(row=row*2, column=col, sticky="w", padx=5, pady=2)
            ttk.Entry(form_grid, textvariable=self.household_vars[key], width=20).grid(
                row=row*2+1, column=col, padx=5, pady=2
            )
        
        # Household buttons
        btn_frame = ttk.Frame(form_frame)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Add Household", command=self.add_household).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Load Households", command=self.load_households).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="View Members", command=self.view_household_members).pack(side="left", padx=5)
        
        # Household list
        household_frame = ttk.LabelFrame(tab, text="Household List")
        household_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.household_tree = DataTreeview(
            household_frame,
            ["ea_code", "structure_no", "household_no", "locality_name", "type_of_residence", "address_detail"]
        )
        self.household_tree.pack(fill="both", expand=True)

    def _create_person_tab(self):
        """Create person management tab (enhanced)"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="👤 Persons")
        
        # Person form (in a paned window for better layout)
        paned = ttk.PanedWindow(tab, orient="vertical")
        paned.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Form frame
        form_frame = ttk.LabelFrame(paned, text="Person Information")
        paned.add(form_frame, weight=1)
        
        # Person fields
        self.person_vars = {
            'person_id': tk.StringVar(),
            'ea_code': tk.StringVar(),
            'structure_no': tk.StringVar(),
            'household_no': tk.StringVar(),
            'line_no': tk.StringVar(),
            'full_name': tk.StringVar(),
            'sex': tk.StringVar(),
            'date_of_birth': tk.StringVar(),
            'age_years': tk.StringVar(),
            'nationality': tk.StringVar(),
            'ethnicity': tk.StringVar(),
            'religion': tk.StringVar(),
            'marital_status': tk.StringVar()
        }
        
        form_grid = ttk.Frame(form_frame)
        form_grid.pack(fill="x", padx=10, pady=5)
        
        person_fields = [
            ("Person ID:", 'person_id'), ("EA Code:", 'ea_code'), ("Structure No:", 'structure_no'),
            ("Household No:", 'household_no'), ("Line No:", 'line_no'), ("Full Name:", 'full_name'),
            ("Sex (M/F):", 'sex'), ("Date of Birth:", 'date_of_birth'), ("Age Years:", 'age_years'),
            ("Nationality:", 'nationality'), ("Ethnicity:", 'ethnicity'), ("Religion:", 'religion'),
            ("Marital Status:", 'marital_status')
        ]
        
        for i, (label, key) in enumerate(person_fields):
            row, col = divmod(i, 3)
            ttk.Label(form_grid, text=label).grid(row=row*2, column=col, sticky="w", padx=5, pady=2)
            ttk.Entry(form_grid, textvariable=self.person_vars[key], width=25).grid(
                row=row*2+1, column=col, padx=5, pady=2
            )
        
        # Person buttons
        btn_frame = ttk.Frame(form_frame)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_frame, text="Insert", command=self.insert_person).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Retrieve", command=self.retrieve_person).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Update", command=self.update_person).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Delete", command=self.delete_person).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Load All", command=self.load_persons).pack(side="left", padx=10)
        
        # Person list
        person_frame = ttk.LabelFrame(paned, text="Person List")
        paned.add(person_frame, weight=2)
        
        self.person_tree = DataTreeview(
            person_frame,
            ["person_id", "full_name", "sex", "age_years", "ea_code", "structure_no", 
             "household_no", "line_no", "locality_name"]
        )
        self.person_tree.pack(fill="both", expand=True)
        self.person_tree.tree.bind('<Double-1>', self._on_person_double_click)

    def _create_reports_tab(self):
        """Create reports and analytics tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📈 Reports")
        
        # Report buttons
        btn_frame = ttk.Frame(tab)
        btn_frame.pack(fill="x", padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Population Summary", command=self.show_population_summary).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Age Distribution", command=self.show_age_distribution).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Gender Statistics", command=self.show_gender_stats).pack(side="left", padx=5)
        
        # Report display area
        report_frame = ttk.LabelFrame(tab, text="Report Results")
        report_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.report_tree = DataTreeview(report_frame, [])
        self.report_tree.pack(fill="both", expand=True)

    def _create_admin_tab(self):
        """Create administration tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="⚙️ Admin")
        
        # Activity log
        log_frame = ttk.LabelFrame(tab, text="Activity Log")
        log_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.admin_tree = DataTreeview(
            log_frame,
            ["log_id", "table_name", "action", "key_value", "username_", "log_time", "details"]
        )
        self.admin_tree.pack(fill="both", expand=True)
        
        # Admin buttons
        admin_btn_frame = ttk.Frame(log_frame)
        admin_btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(admin_btn_frame, text="Refresh Log", command=self.load_activity_log).pack(side="left", padx=5)
        ttk.Button(admin_btn_frame, text="Clear Log", command=self.clear_activity_log).pack(side="left", padx=5)

    # Event handlers and methods
    def connect_db(self):
        """Connect to Oracle database"""
        try:
            if self.db:
                self.db.close()
            
            self.db = OracleDB(
                self.host_var.get(), int(self.port_var.get()),
                self.service_var.get(), self.user_var.get(), self.pwd_var.get()
            )
            
            self.conn_status.config(text="Connected", foreground="green")
            messagebox.showinfo("Success", "Connected to Oracle database")
            
            # Load initial data
            self.refresh_all_data()
            
        except Exception as e:
            messagebox.showerror("Connection Error", f"Failed to connect: {e}")
            self.conn_status.config(text="Connection Failed", foreground="red")

    def disconnect_db(self):
        """Disconnect from database"""
        if self.db:
            self.db.close()
            self.db = None
        self.conn_status.config(text="Disconnected", foreground="orange")

    def refresh_all_data(self):
        """Refresh all data in the application"""
        if not self._check_connection():
            return
        
        try:
            # Load geography data
            regions = self.db.get_regions()
            self.region_combo['values'] = [r['region_code'] for r in regions]
            
            # Load enumeration areas
            eas = self.db.get_enumeration_areas()
            self.ea_tree.load_data(eas)
            
            # Load recent activity
            activity = self.db.get_activity_log(20)
            self.activity_tree.load_data(activity)
            
            # Update dashboard stats
            self._update_dashboard_stats()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to refresh data: {e}")

    def _update_dashboard_stats(self):
        """Update dashboard statistics"""
        if not self.db:
            return
        
        try:
            stats_text = "=== DATABASE STATISTICS ===\n\n"
            
            # Count records in major tables
            tables = ['person', 'household', 'enumeration_area', 'locality']
            for table in tables:
                result = self.db.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                count = result[0]['count'] if result else 0
                stats_text += f"{table.replace('_', ' ').title()}: {count:,}\n"
            
            # Gender distribution
            gender_stats = self.db.execute_query(
                """SELECT sex, COUNT(*) as count FROM person 
                   WHERE sex IS NOT NULL GROUP BY sex ORDER BY sex"""
            )
            stats_text += "\n=== GENDER DISTRIBUTION ===\n"
            for row in gender_stats:
                stats_text += f"{row['sex']}: {row['count']:,}\n"
            
            self.stats_text.delete(1.0, "end")
            self.stats_text.insert(1.0, stats_text)
            
        except Exception as e:
            self.stats_text.delete(1.0, "end")
            self.stats_text.insert(1.0, f"Error loading statistics: {e}")

    def _check_connection(self) -> bool:
        """Check if database is connected"""
        if not self.db:
            messagebox.showwarning("Not Connected", "Please connect to the database first")
            return False
        return True

    def _on_region_selected(self, event):
        """Handle region selection"""
        if not self._check_connection():
            return
        
        region_code = self.region_var.get()
        if region_code:
            districts = self.db.get_districts(region_code)
            self.district_combo['values'] = [d['district_code'] for d in districts]

    def _on_person_double_click(self, event):
        """Handle double-click on person in tree"""
        selected = self.person_tree.get_selected_row()
        if selected:
            # Fill form with selected person data
            for key, var in self.person_vars.items():
                var.set(selected.get(key, ''))

    # Household methods
    def add_household(self):
        """Add a new household"""
        if not self._check_connection():
            return
        
        try:
            data = {key: var.get() or None for key, var in self.household_vars.items()}
            
            # Validate required fields
            required = ['ea_code', 'structure_no', 'household_no', 'type_of_residence']
            for field in required:
                if not data[field]:
                    messagebox.showwarning("Validation", f"{field} is required")
                    return
            
            self.db.insert_household(data)
            messagebox.showinfo("Success", "Household added successfully")
            self.load_households()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add household: {e}")

    def load_households(self):
        """Load households into the tree"""
        if not self._check_connection():
            return
        
        try:
            households = self.db.get_households()
            self.household_tree.load_data(households)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load households: {e}")

    def view_household_members(self):
        """View members of selected household"""
        selected = self.household_tree.get_selected_row()
        if not selected:
            messagebox.showwarning("Selection", "Please select a household first")
            return
        
        if not self._check_connection():
            return
        
        try:
            household_key = (selected['ea_code'], selected['structure_no'], selected['household_no'])
            persons = self.db.get_persons(household_key)
            
            # Switch to person tab and load the data
            self.notebook.select(3)  # Person tab index
            self.person_tree.load_data(persons)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load household members: {e}")

    # Person methods (enhanced)
    def insert_person(self):
        """Insert a new person"""
        if not self._check_connection():
            return
        
        try:
            data = {}
            for key, var in self.person_vars.items():
                value = var.get()
                if key == 'date_of_birth' and value:
                    try:
                        data[key] = dt.datetime.strptime(value, '%Y-%m-%d').date()
                    except ValueError:
                        messagebox.showwarning("Invalid Date", "Date must be in YYYY-MM-DD format")
                        return
                elif key in ['line_no', 'age_years'] and value:
                    try:
                        data[key] = int(value)
                    except ValueError:
                        messagebox.showwarning("Invalid Number", f"{key} must be a number")
                        return
                else:
                    data[key] = value or None
            
            new_id = self.db.sp_insert_person(data)
            self.person_vars['person_id'].set(str(new_id))
            messagebox.showinfo("Success", f"Person inserted with ID: {new_id}")
            self.load_persons()
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to insert person: {e}")

    def retrieve_person(self):
        """Retrieve person by ID"""
        if not self._check_connection():
            return
        
        person_id = self.person_vars['person_id'].get()
        if not person_id:
            messagebox.showwarning("Input Required", "Please enter Person ID")
            return
        
        try:
            data = self.db.sp_get_person_by_id(int(person_id))
            if not data:
                messagebox.showinfo("Not Found", "Person not found")
                return
            
            # Fill form fields
            for key, var in self.person_vars.items():
                value = data.get(key, '')
                if isinstance(value, dt.date):
                    value = value.strftime('%Y-%m-%d')
                var.set(str(value) if value is not None else '')
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to retrieve person: {e}")

    def update_person(self):
        """Update person record"""
        # Implementation similar to original but with enhanced error handling
        messagebox.showinfo("Not Implemented", "Update functionality to be implemented")

    def delete_person(self):
        """Delete person record"""
        # Implementation similar to original but with enhanced error handling
        messagebox.showinfo("Not Implemented", "Delete functionality to be implemented")

    def load_persons(self):
        """Load all persons into the tree"""
        if not self._check_connection():
            return
        
        try:
            persons = self.db.get_persons(limit=200)
            self.person_tree.load_data(persons)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load persons: {e}")

    # Report methods
    def show_population_summary(self):
        """Show population summary report"""
        if not self._check_connection():
            return
        
        try:
            data = self.db.get_population_summary()
            columns = ["region_name", "district_name", "population", "male_count", "female_count", "avg_age"]
            
            # Update report tree columns
            self.report_tree.columns = columns
            self.report_tree._init_widgets()
            self.report_tree.load_data(data)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to generate report: {e}")

    def show_age_distribution(self):
        """Show age distribution report"""
        if not self._check_connection():
            return
        
        try:
            data = self.db.get_age_distribution()
            columns = ["age_group", "count", "percentage"]
            
            self.report_tree.columns = columns
            self.report_tree._init_widgets()
            self.report_tree.load_data(data)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to generate report: {e}")

    def show_gender_stats(self):
        """Show gender statistics"""
        if not self._check_connection():
            return
        
        try:
            data = self.db.execute_query(
                """SELECT sex, COUNT(*) as count,
                          ROUND(AVG(age_years), 1) as avg_age,
                          MIN(age_years) as min_age,
                          MAX(age_years) as max_age
                   FROM person WHERE sex IS NOT NULL AND age_years IS NOT NULL
                   GROUP BY sex ORDER BY sex"""
            )
            columns = ["sex", "count", "avg_age", "min_age", "max_age"]
            
            self.report_tree.columns = columns
            self.report_tree._init_widgets()
            self.report_tree.load_data(data)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to generate report: {e}")

    # Admin methods
    def load_activity_log(self):
        """Load activity log"""
        if not self._check_connection():
            return
        
        try:
            logs = self.db.get_activity_log(100)
            self.admin_tree.load_data(logs)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load activity log: {e}")

    def clear_activity_log(self):
        """Clear activity log (with confirmation)"""
        if not messagebox.askyesno("Confirm", "Are you sure you want to clear the activity log?"):
            return
        
        if not self._check_connection():
            return
        
        try:
            self.db.execute_non_query("DELETE FROM activity_log")
            messagebox.showinfo("Success", "Activity log cleared")
            self.load_activity_log()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to clear log: {e}")

    # Utility methods
    def show_db_stats(self):
        """Show database statistics dialog"""
        if not self._check_connection():
            return
        
        stats_window = tk.Toplevel(self)
        stats_window.title("Database Statistics")
        stats_window.geometry("600x400")
        
        text = tk.Text(stats_window, wrap="word")
        scrollbar = ttk.Scrollbar(stats_window, command=text.yview)
        text.configure(yscrollcommand=scrollbar.set)
        
        try:
            # Get comprehensive statistics
            stats_content = "=== COMPREHENSIVE DATABASE STATISTICS ===\n\n"
            
            # Table counts
            tables = ['region', 'district', 'subdistrict', 'locality', 'enumeration_area',
                     'structure', 'household', 'person', 'household_roster']
            
            for table in tables:
                try:
                    result = self.db.execute_query(f"SELECT COUNT(*) as count FROM {table}")
                    count = result[0]['count'] if result else 0
                    stats_content += f"{table.replace('_', ' ').title()}: {count:,}\n"
                except:
                    stats_content += f"{table.replace('_', ' ').title()}: Error\n"
            
            text.insert(1.0, stats_content)
            
        except Exception as e:
            text.insert(1.0, f"Error loading statistics: {e}")
        
        text.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def show_about(self):
        """Show about dialog"""
        about_text = """
Census Database Management System
Version 2.0

A comprehensive application for managing census data with Oracle database backend.

Features:
• Geographic hierarchy management
• Household and person records
• Advanced reporting and analytics
• Activity logging and monitoring
• Data export capabilities

Built with Python and Tkinter
Database: Oracle 19c+
        """
        messagebox.showinfo("About", about_text)

if __name__ == "__main__":
    app = CensusApp()
    app.mainloop()