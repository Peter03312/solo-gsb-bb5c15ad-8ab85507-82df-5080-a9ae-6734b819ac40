from contextlib import asynccontextmanager

from fastapi import FastAPI, Response

from . import config, db, services
from .schemas import (
    AssignRequest,
    ExperimentCreate,
    ExperimentUpdate,
    ExposureRequest,
    LayerCreate,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.create_pool(config.DATABASE_URL)
    yield
    await db.close_pool()


app = FastAPI(
    title="Anonymous Experiment Traffic Split Service",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {"service": "experiment-traffic-split", "docs": "/docs"}


@app.get("/health")
async def health():
    async with db.get_pool().acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok"}


# ---------------- layers ----------------

@app.post("/layers", status_code=201, tags=["layers"])
async def create_layer(body: LayerCreate):
    return await services.create_layer(db.get_pool(), body)


@app.get("/layers/{layer_key}", tags=["layers"])
async def get_layer(layer_key: str):
    return await services.get_layer(db.get_pool(), layer_key)


@app.get("/layers/{layer_key}/experiments", tags=["experiments"])
async def list_experiments(layer_key: str):
    return await services.list_experiments(db.get_pool(), layer_key)


@app.get("/layers/{layer_key}/users/{user_key}/assignment", tags=["assignments"])
async def current_assignment(layer_key: str, user_key: str):
    return await services.get_current_assignment(db.get_pool(), layer_key, user_key)


# ---------------- experiments ----------------

@app.post("/layers/{layer_key}/experiments", status_code=201, tags=["experiments"])
async def create_experiment(layer_key: str, body: ExperimentCreate):
    return await services.create_experiment(db.get_pool(), layer_key, body)


@app.get("/experiments/{experiment_id}", tags=["experiments"])
async def get_experiment(experiment_id: int):
    return await services.get_experiment(db.get_pool(), experiment_id)


@app.put("/experiments/{experiment_id}", tags=["experiments"])
async def update_experiment(experiment_id: int, body: ExperimentUpdate):
    return await services.update_experiment(db.get_pool(), experiment_id, body)


@app.post("/experiments/{experiment_id}/publish", tags=["experiments"])
async def publish_experiment(experiment_id: int):
    return await services.publish_experiment(db.get_pool(), experiment_id)


@app.post("/experiments/{experiment_id}/end", tags=["experiments"])
async def end_experiment(experiment_id: int):
    return await services.end_experiment(db.get_pool(), experiment_id)


# ---------------- assignment ----------------

@app.post("/assign", tags=["assignments"])
async def assign(body: AssignRequest):
    return await services.assign(db.get_pool(), body)


@app.get("/assignments/{assignment_id}", tags=["assignments"])
async def get_assignment(assignment_id: int):
    return await services.get_assignment(db.get_pool(), assignment_id)


# ---------------- exposures ----------------

@app.post("/exposures", tags=["exposures"])
async def register_exposure(body: ExposureRequest, response: Response):
    payload, status = await services.register_exposure(db.get_pool(), body)
    response.status_code = status
    return payload


@app.get("/assignments/{assignment_id}/exposures", tags=["exposures"])
async def list_exposures(assignment_id: int):
    return await services.list_exposures(db.get_pool(), assignment_id)
