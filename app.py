from pathlib import Path

from fastapi import FastAPI, Form
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from common.config import Config


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Hajimi King Web")

# Mount static directory for assets
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
def serve_index() -> FileResponse:
    index_file = STATIC_DIR / "index.html"
    return FileResponse(path=str(index_file))


@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)) -> Response:
    if username == Config.AUTH_USER and password == Config.AUTH_PASSWORD:
        return RedirectResponse(url="/dashboard", status_code=303)
    return HTMLResponse(content="Invalid credentials", status_code=401)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> FileResponse:
    dashboard_file = STATIC_DIR / "dashboard.html"
    return FileResponse(path=str(dashboard_file))


