from fastapi import APIRouter

router = APIRouter()


@router.get("/metrics")
def metrics():
    return {"message": "Metrics endpoint"}
