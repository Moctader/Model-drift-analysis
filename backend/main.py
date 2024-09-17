import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import model, kafka, cassandra

# INITIALIZE COMPONENTS
app = FastAPI()
API_PORT = 3003

# CORS MIDDLEWARE
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CUSTOM ROUTES
# app.include_router(model.router)
app.include_router(kafka.router)
app.include_router(cassandra.router)

# LAUNCH SERVER
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=API_PORT,
        log_level="info",
        reload=True,
        # workers=NUM
    )
