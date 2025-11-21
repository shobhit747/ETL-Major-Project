{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3a0205f3-1c40-4413-ab4c-55fe8e8678e3",
   "metadata": {},
   "outputs": [],
   "source": [
    "import time\n",
    "import psutil\n",
    "import pandas as pd\n",
    "from pathlib import Path\n",
    "from transform import transform_chunk\n",
    "\n",
    "\n",
    "def run_sequential_benchmark(input_file: Path, chunk_size: int, config) -> dict:\n",
    "    start = time.perf_counter()\n",
    "    cpu_start = psutil.cpu_percent(interval=None)\n",
    "    mem_start = psutil.virtual_memory().percent\n",
    "\n",
    "    out_frames = []\n",
    "    reader = pd.read_csv(input_file, chunksize=chunk_size, dtype=config.DTYPES, low_memory=False)\n",
    "    for chunk in reader:\n",
    "        df_t = transform_chunk(\n",
    "            chunk,\n",
    "            required_cols=config.REQUIRED_COLUMNS,\n",
    "            dtypes=config.DTYPES,\n",
    "            enable_features=config.ENABLE_FEATURES,\n",
    "            drop_duplicates=config.DROP_DUPLICATES,\n",
    "            drop_na_rows=config.DROP_NA_ROWS\n",
    "        )\n",
    "        out_frames.append(df_t)\n",
    "\n",
    "    df = pd.concat(out_frames, ignore_index=True)\n",
    "    # Optionally write for parity with parallel run\n",
    "    df.to_parquet(config.OUTPUT_DIR / \"sequential_output.parquet\", index=False)\n",
    "\n",
    "    end = time.perf_counter()\n",
    "    cpu_end = psutil.cpu_percent(interval=None)\n",
    "    mem_end = psutil.virtual_memory().percent\n",
    "\n",
    "    return {\n",
    "        \"elapsed_seconds\": round(end - start, 3),\n",
    "        \"cpu_percent_start\": cpu_start,\n",
    "        \"cpu_percent_end\": cpu_end,\n",
    "        \"mem_percent_start\": mem_start,\n",
    "        \"mem_percent_end\": mem_end,\n",
    "        \"rows\": len(df),\n",
    "        \"columns\": list(df.columns),\n",
    "    }\n",
    "\n",
    "\n",
    "def compare_results(par_metrics: dict, seq_metrics: dict) -> dict:\n",
    "    return {\n",
    "        \"parallel_time_s\": par_metrics[\"elapsed_seconds\"],\n",
    "        \"sequential_time_s\": seq_metrics[\"elapsed_seconds\"],\n",
    "        \"speedup_x\": round(seq_metrics[\"elapsed_seconds\"] / par_metrics[\"elapsed_seconds\"], 3)\n",
    "                      if par_metrics[\"elapsed_seconds\"] > 0 else None,\n",
    "        \"parallel_mem_end_pct\": par_metrics[\"mem_percent_end\"],\n",
    "        \"sequential_mem_end_pct\": seq_metrics[\"mem_percent_end\"],\n",
    "    }"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.13.5"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
