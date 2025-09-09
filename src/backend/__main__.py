from backend.constants import BACKEND_PORT
from backend.endpoints import app
from core.log import get_logger

logger = get_logger(__name__)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=BACKEND_PORT)
