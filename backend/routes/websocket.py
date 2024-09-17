from fastapi import APIRouter, WebSocket
import utils.websocket as websocket

# CREATE AUXILLARY OBJECTS
router = APIRouter()
socket_manager = websocket.create_socket_manager()

@router.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):

    # OPEN SOCKET CONNECTION
    await websocket.accept()

    # WAIT FOR HASH INTEREST
    try:
        submission_hash = await websocket.receive_text()
        await socket_manager.add(submission_hash, websocket)
        await websocket.receive_text()
    
    # RESOLVE EXCEPTIONS SILENTLY
    except:
        # print('WS EXCEPT TRIGGERED')
        pass