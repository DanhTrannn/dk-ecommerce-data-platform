from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from platform_app.credential_export.auth import authenticate, read_key
from platform_app.credential_export.schemas import Assignment
from platform_app.credential_export.storage import ExportError, ExportSettings, ExportStore


def create_app(settings=None, *, store=None):
    settings = settings or ExportSettings()
    store = store or ExportStore(settings)
    application = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @application.exception_handler(ExportError)
    async def export_error(request, exc):
        return JSONResponse(status_code=exc.status_code, content={'detail': exc.message})

    async def authorized(request, payload=None):
        try:
            authorization = request.headers.get('authorization', '')
            if not authorization.startswith('Bearer '):
                raise ValueError('Missing authorization')
            return authenticate(read_key(settings.key_file), authorization[7:], request.method, request.url.path, payload)
        except Exception:
            raise ExportError(401, 'Invalid exporter authorization') from None

    @application.get('/health/live')
    def health():
        return {'status': 'ok'}

    @application.get('/v1/workloads')
    async def snapshot(request: Request):
        await authorized(request)
        return store.snapshot()

    @application.post('/v1/workloads')
    async def apply(request: Request):
        try:
            payload = await request.json()
        except Exception:
            raise ExportError(400, 'Invalid request') from None
        actor = await authorized(request, payload)
        try:
            assignment = Assignment.model_validate(payload)
        except ValidationError:
            raise ExportError(422, 'Invalid workload assignment') from None
        return store.apply(assignment, actor)

    return application


app = create_app()
