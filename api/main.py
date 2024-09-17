from pathlib import Path
import uvicorn
from fastapi.responses import RedirectResponse
from core.registrar import register_app

app = register_app()


@app.get("/")
async def home():
    return RedirectResponse(url="/docs")

if __name__ == '__main__':
    config = uvicorn.Config(
        app=f'{Path(__file__).stem}:app', reload=True, host="0.0.0.0", port=8050)
    server = uvicorn.Server(config)
    server.run()
