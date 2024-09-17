import websockets
import asyncio
import json


class WebSocketClient:
    def __init__(self, url):
        self.url = url
        self.reconnect_attempts = 2
        self.reconnect_delay = 1
        self.websocket = None

    async def connect(self, on_message):
        for attempt in range(self.reconnect_attempts):
            try:
                print(
                    f"Connecting to WebSocket: {self.url}. "
                    f"Attempt {attempt + 1}/{self.reconnect_attempts}"
                )
                self.websocket = await websockets.connect(self.url)
                await self.listen(on_message)
                break
            except Exception as e:
                print(
                    f"WebSocket connection failed: {e}. "
                    f"Attempt {attempt + 1}/{self.reconnect_attempts}"
                )
                await asyncio.sleep(self.reconnect_delay)
                if attempt == self.reconnect_attempts - 1:
                    print("Max reconnection attempts reached. Exiting.")
                    break

    async def listen(self, on_message):
        try:
            print("Connected to WebSocket")
            while True:
                try:
                    message = await self.websocket.recv()
                    await on_message(message)
                except websockets.ConnectionClosedError as e:
                    print(f"""Connection closed with error: {
                          e}. Reconnecting...""")
                    break
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            await self.close()

    async def send_message(self, message):
        if self.websocket:
            await self.websocket.send(json.dumps(message))

    async def close(self):
        if self.websocket:
            await self.websocket.close()
