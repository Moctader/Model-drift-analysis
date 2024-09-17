import json

class create_socket_manager():
    def __init__(self):
        self.connections = {}

    # ADD NEW SUBMISSION WEBSOCKET
    async def add(self, submission_hash, websocket):

        # HASH ALREADY EXISTS -- APPEND
        if submission_hash in self.connections:
            self.connections[submission_hash].append(websocket)

        # OTHERWISE, CREATE HASH
        else:
            self.connections[submission_hash] = [websocket]

    # REMOVE EVERY SUBMISSION SPECIFIC WEBSOCKET
    async def remove(self, submission_hash: str):
        if submission_hash in self.connections:
            for socket in self.connections[submission_hash]:
                try:
                    await socket.close()
                except:
                    print('SOCKET REMOVAL FAILED')
                    pass

    # MESSAGE AWAITING WEBSOCKETS
    async def publish(self, submission_hash: str, message: dict):
        if submission_hash in self.connections:

            encoded = json.dumps(message)

            for socket in self.connections[submission_hash]:
                try:
                    await socket.send_text(encoded)
                except:
                    print('SOCKET MESSAGING FAILED')