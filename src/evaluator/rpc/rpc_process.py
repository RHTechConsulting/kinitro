import asyncio
import capnp
from ray.util.queue import Queue
from evaluator.rpc.client import AgentClient


async def rpc_process(host: str, port: int, send_queue: Queue, recv_queue: Queue):
    print(f"Starting RPC process for {host}:{port}")
    agent = AgentClient(host, port)
    await agent.connect()
    
    pong = await agent.ping("rpc-process-ping")
    print(f"RPC process for {host}:{port} connected, startup ping={pong}")
    
    while True:
        try:
            # Check if queue is empty and print status
            if recv_queue.empty():
                await asyncio.sleep(0.05)
                continue
            
            message = await recv_queue.get_async()
            print(f"RPC process for {host}:{port} received message={message}")
            response = await agent.ping(message)
            print(f"RPC process for {host}:{port} sent message={response}")
            await send_queue.put_async(response)
        except Exception as e:
            print(f"Error: {e}")
            break

def start_rpc_process(host: str, port: int, send_queue: Queue, recv_queue: Queue):
    asyncio.run(capnp.run(rpc_process(host, port, send_queue, recv_queue)))