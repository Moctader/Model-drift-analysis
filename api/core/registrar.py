from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi_pagination import add_pagination
from fastapi.middleware.cors import CORSMiddleware
from routes import financial_api
from core.settings import settings
from core.logging import setup_logging


@asynccontextmanager
async def register_init(app: FastAPI):
    yield


def register_app():
    app = FastAPI(
        debug=settings.DEBUG,
        title=settings.PROJECT_NAME,
        version=settings.VERSION,
        description=settings.DESCRIPTION,
        docs_url=settings.DOCS_URL,
        redoc_url=settings.REDOCS_URL,
        openapi_url=settings.OPENAPI_URL,
        lifespan=register_init,
    )

    # Setup logging
    setup_logging()

    # Middleware
    register_middleware(app)

    # Routes
    register_router(app)

    # Pagination
    add_pagination(app)

    return app


def register_middleware(api: FastAPI):
    api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def register_router(api: FastAPI):
    api.include_router(
        financial_api.router,
        prefix=settings.API_PREFIX,
        tags=["financial"],
    )
