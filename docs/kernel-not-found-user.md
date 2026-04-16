# Fixing "Kernel Not Found" in Marimo Notebooks

When you open a notebook and see "kernel not found" or a blank page instead
of your code, follow these steps.

## Step 1: Hard Refresh Your Browser

This is the most common fix. The browser may be showing a cached error page.

- **Mac:** `Cmd + Shift + R`
- **Windows / Linux:** `Ctrl + Shift + R`

If the notebook loads after refreshing, you're done.

## Step 2: Check Your Setup

Ask the assistant to run `check_setup`. Look at the output for:

- **`marimo_available: false`** — marimo isn't installed. Install it:
  ```
  pip install marimo
  ```

- **`VERSION MISMATCH` in notes** — you have two different versions of
  marimo installed. The notes will tell you which versions and how to fix
  it. Usually:
  ```
  pip install marimo==<the version shown>
  ```

- **`marimo_system_version` and `marimo_library_version` match** — the
  problem is likely something else. Continue to Step 3.

## Step 3: Re-open the Notebook

Ask the assistant to close and re-open the notebook:

```
open_notebook(run_id="<your_run_id>")
```

This will kill any stale process on the port and start fresh.

## Step 4: Re-run Your Code

If the notebook's virtual environment was cleaned up (you'll see an error
about "Virtual environment not found"), re-run your code to recreate it:

```
rerun(run_id="<your_run_id>")
```

Then open the new run's notebook.

## Step 5: Open Manually

If all else fails, you can open the notebook directly from your terminal:

```bash
marimo edit <notebook_path> --no-sandbox --no-token
```

The `--no-sandbox` flag is important — without it, marimo may try to create
its own environment that conflicts with the one your code was run in.

You can find the `notebook_path` by asking the assistant to run
`get_run(run_id="<your_run_id>")`.

## Why Does This Happen?

"Kernel not found" usually means marimo can't set up the Python environment
to run your notebook. The most common reasons:

- **Version mismatch** — the notebook was created with one version of marimo
  but you're opening it with a different version that doesn't understand the
  format.
- **Browser cache** — your browser is showing an old error page even though
  the server restarted successfully.
- **Stale process** — a previous marimo server is still running on the same
  port and serving an old notebook.

The system is designed to handle all of these automatically, but edge cases
can still occur, especially after upgrades or environment changes.
