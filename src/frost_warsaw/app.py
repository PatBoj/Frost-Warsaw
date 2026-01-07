import tkinter as tk
from tkinter import ttk
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from datetime import datetime
import geopandas as gpd
from shapely import wkt
import osmnx as ox

# --- Load data ---
df = pd.read_csv("src/frost_warsaw/plot_data.csv")
df["geometry"] = df["geometry"].apply(wkt.loads)
df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:2180")
df["x"] = df.geometry.x
df["y"] = df.geometry.y
df["time"] = pd.to_datetime(df["time"])

# --- Tkinter app ---
root = tk.Tk()
root.title("Time + Afterglow Plot")
root.geometry("950x750")

filter_gminy = ["powiat " + item for item in [
    "wołomiński",
    "pruszkowski",
    "miński",
    "otwocki",
    "piaseczyński",
    "warszawski zachodni",
    "nowodworski",
    "legionowski",
    ]]

warsaw = ox.geocode_to_gdf("Warsaw, Poland")
districts = ox.features_from_place(
    "Warsaw, Poland",
    {"boundary": "administrative", "admin_level": "9"}
)
gminy = ox.features_from_place(
    "Masovian Voivodeship, Poland",
    {"admin_level": "6"}
)

districts = districts[districts["admin_level"] == "9"]
districts = districts[districts.geom_type == "Polygon"]
districts = districts[districts["boundary"].notnull()]
districts = districts[districts["wikidata"].notnull()]

warsaw = warsaw.set_crs(epsg=4326)
warsaw = warsaw.to_crs(epsg=2180)
districts = districts.set_crs(epsg=4326)
districts = districts.to_crs(epsg=2180)

# --- Plot figure ---
fig, ax = plt.subplots(figsize=(12, 12))
warsaw.boundary.plot(ax=ax, linewidth=2.5, color="black", rasterized=True)
districts.boundary.plot(ax=ax, linewidth=0.8, color="gray", alpha=0.9, rasterized=True)
minx, miny, maxx, maxy = warsaw.total_bounds
minx *= 0.9999; miny *= 0.99999; maxx *= 1.0001; maxy *= 1.00001
ax.set_xlim(minx, maxx)
ax.set_ylim(miny, maxy)

canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

# --- Controls frame ---
frame = tk.Frame(root)
frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=10)

show_all_var = tk.IntVar()
show_all_checkbox = tk.Checkbutton(frame, text="Show all", variable=show_all_var)
show_all_checkbox.grid(row=0, column=0, padx=5)

id_values = ["All"] + sorted(df["id"].dropna().unique())
id_var = tk.StringVar(value="All")
id_label = tk.Label(frame, text="Filter by ID")
id_label.grid(row=0, column=1, padx=5)
id_dropdown = ttk.Combobox(frame, textvariable=id_var, values=id_values, state="readonly")
id_dropdown.grid(row=0, column=2, padx=5)

slider_label = tk.Label(frame, text="Time")
slider_label.grid(row=0, column=3, padx=5)

time_slider = tk.Scale(frame, from_=0, to=len(df)-1, orient=tk.HORIZONTAL, length=400)
time_slider.set(len(df)-1)
time_slider.grid(row=0, column=4, padx=5)

# --- Update slider range based on selected ID ---
def update_slider_range():
    if id_var.get() != "All":
        df_filtered = df[df["id"] == id_var.get()]
    else:
        df_filtered = df.copy()
    if df_filtered.empty:
        time_slider.config(from_=0, to=0)
        time_slider.set(0)
    else:
        time_slider.config(from_=0, to=len(df_filtered)-1)
        time_slider.set(len(df_filtered)-1)

# --- Update plot function ---
def update_plot(*args):
    ax.clear()

    # Filter by ID
    if id_var.get() != "All":
        df_filtered = df[df["id"] == id_var.get()]
    else:
        df_filtered = df.copy()

    if df_filtered.empty:
        canvas.draw()
        return

    # Filter by time (slider)
    if not show_all_var.get():
        idx = time_slider.get()
        df_filtered = df_filtered.iloc[:idx+1]

    if df_filtered.empty:
        canvas.draw()
        return

    n_points = len(df_filtered)
    alphas = np.linspace(0.05, 1.0, n_points)

    # Scatter + line
    ax.scatter(df_filtered['x'], df_filtered['y'], color=(0.12,0.46,0.7,1), s=30, alpha=1.0)
    ax.plot(df_filtered['x'], df_filtered['y'], color=(0.12,0.46,0.7,0.3))

    # Highlight newest point
    latest = df_filtered.iloc[-1]
    ax.scatter(latest['x'], latest['y'], color='red', s=60)

    # Update slider label to show latest timestamp
    slider_label.config(text=f"Time: {latest['time']}")

    ax.set_title(f"y vs x (up to {latest['time']})")
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    canvas.draw()

# --- Bind events ---
id_var.trace_add('write', lambda *args: (update_slider_range(), update_plot()))
id_dropdown.bind("<<ComboboxSelected>>", lambda e: (update_slider_range(), update_plot()))
time_slider.config(command=lambda val: update_plot())
show_all_var.trace_add('write', lambda *args: update_plot())

# --- Initialize ---
update_slider_range()
update_plot()

root.mainloop()
