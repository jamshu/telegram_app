import os
import asyncio
import io
import time
import math
from collections import deque
from tqdm import tqdm
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename
from dotenv import load_dotenv

# Load .env file
load_dotenv()
api_id = os.getenv("api_id")
api_hash = os.getenv("api_hash")
phone_number = os.getenv("phone_number")

DOWNLOADS_FOLDER = "downloads"  # Define downloads folder

# Performance tuning parameters
MAX_CONCURRENT_DOWNLOADS = 24  # Increased max concurrent chunks
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB base chunk size
REQUEST_SIZE = 1024 * 1024  # 1MB request size for iter_download
MAX_RETRIES = 5
INITIAL_RETRY_DELAY = 0.5  # Start with a shorter delay
DC_PERFORMANCE_PROFILE = True  # Enable adaptive DC performance tracking

# Ensure downloads directory exists
os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)

# Track datacenter performance
dc_performance = {}


class AdaptiveChunkManager:
    def __init__(self, file_size, max_connections=MAX_CONCURRENT_DOWNLOADS):
        self.file_size = file_size
        self.max_connections = max_connections
        self.completed_chunks = 0
        self.start_time = time.time()
        self.chunk_times = deque(maxlen=10)  # Keep last 10 chunk times
        self.initial_connections = self._calculate_initial_connections()
        self.current_connections = self.initial_connections
        self.semaphore = asyncio.Semaphore(self.current_connections)
        
    def _calculate_initial_connections(self):
        # Calculate based on file size with diminishing returns for very large files
        if self.file_size < 10 * 1024 * 1024:  # < 10MB
            return 4
        elif self.file_size < 100 * 1024 * 1024:  # < 100MB
            return 8
        elif self.file_size < 1 * 1024 * 1024 * 1024:  # < 1GB
            return 16
        else:
            return min(self.max_connections, 16 + int(math.log2(self.file_size / (1 * 1024 * 1024 * 1024)) * 4))
    
    def get_chunk_size(self):
        return max(1024 * 1024, self.file_size // self.current_connections)
    
    def chunk_completed(self, time_taken):
        self.completed_chunks += 1
        self.chunk_times.append(time_taken)
        
        # Adaptive connection scaling every 5 chunks
        if self.completed_chunks % 5 == 0 and len(self.chunk_times) >= 5:
            avg_time = sum(self.chunk_times) / len(self.chunk_times)
            
            # If chunks are taking too long, reduce connections
            if avg_time > 5.0 and self.current_connections > 4:
                self.current_connections = max(4, self.current_connections - 2)
                self.semaphore = asyncio.Semaphore(self.current_connections)
                
            # If chunks are completing quickly, increase connections
            elif avg_time < 1.0 and self.current_connections < self.max_connections:
                self.current_connections = min(self.max_connections, self.current_connections + 2)
                self.semaphore = asyncio.Semaphore(self.current_connections)
    
    async def get_semaphore(self):
        return self.semaphore


async def download_chunk(client, message, start, end, output_file, progress_bar, chunk_manager, chunk_id, retries=0):
    chunk_start_time = time.time()
    semaphore = await chunk_manager.get_semaphore()
    
    async with semaphore:
        try:
            # Get the DC ID being used
            dc_id = message.media.document.dc_id
            if dc_id not in dc_performance:
                dc_performance[dc_id] = {"success": 0, "failure": 0, "avg_speed": 0}
            
            # Calculate the chunk size
            chunk_size = end - start + 1
            
            # Use iter_download to download the specific chunk
            buffer = io.BytesIO()
            downloaded = 0
            chunk_download_start = time.time()
            
            async for chunk in client.iter_download(
                message.media,
                offset=start,
                request_size=REQUEST_SIZE
            ):
                # Write to buffer
                buffer.write(chunk)
                downloaded += len(chunk)
                progress_bar.update(len(chunk))
                
                # Break if we've downloaded enough
                if downloaded >= chunk_size:
                    break
            
            # Calculate download speed for this chunk
            download_time = time.time() - chunk_download_start
            if download_time > 0:
                chunk_speed = chunk_size / (1024 * 1024 * download_time)  # MB/s
                
                # Update DC performance metrics
                dc_perf = dc_performance[dc_id]
                dc_perf["success"] += 1
                dc_perf["avg_speed"] = (dc_perf["avg_speed"] * (dc_perf["success"] - 1) + chunk_speed) / dc_perf["success"]
            
            # Write the buffer to file at the correct position
            with open(output_file, "r+b") as f:
                f.seek(start)
                f.write(buffer.getvalue()[:chunk_size])  # Only write up to chunk_size
            
            # Report completion to chunk manager
            chunk_manager.chunk_completed(time.time() - chunk_start_time)
                
        except Exception as e:
            # Update DC failure metrics
            if 'dc_id' in locals() and dc_id in dc_performance:
                dc_performance[dc_id]["failure"] += 1
            
            if retries < MAX_RETRIES:
                # Exponential backoff with jitter
                delay = INITIAL_RETRY_DELAY * (2 ** retries) * (0.5 + 0.5 * (asyncio.get_event_loop().time() % 1))
                await asyncio.sleep(delay)
                await download_chunk(client, message, start, end, output_file, 
                                   progress_bar, chunk_manager, chunk_id, retries + 1)
            else:
                print(f"Failed to download chunk {chunk_id} ({start}-{end}) after {MAX_RETRIES} retries: {e}")


async def download_file_concurrently(client, message, output_file, file_size):
    # Create adaptive chunk manager
    chunk_manager = AdaptiveChunkManager(file_size)
    initial_connections = chunk_manager.initial_connections
    
    # Calculate initial chunk size
    base_chunk_size = chunk_manager.get_chunk_size()
    
    # Create progress bar with smoother update interval
    progress_bar = tqdm(
        total=file_size, 
        desc="Downloading", 
        unit='B', 
        unit_scale=True, 
        unit_divisor=1024,
        mininterval=0.5
    )
    
    # Create and gather download tasks
    tasks = []
    for i in range(initial_connections):
        start = i * base_chunk_size
        
        # Make sure the last chunk gets the remainder
        if i == initial_connections - 1:
            end = file_size - 1
        else:
            end = start + base_chunk_size - 1
        
        task = asyncio.create_task(
            download_chunk(client, message, start, end, output_file, 
                         progress_bar, chunk_manager, i)
        )
        tasks.append(task)
    
    # Start timing
    start_time = time.time()
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    # Calculate and show stats
    elapsed = time.time() - start_time
    speed_mbps = file_size / (1024 * 1024 * elapsed) if elapsed > 0 else 0
    
    progress_bar.close()
    
    # Show performance stats
    print(f"Download completed in {elapsed:.1f} seconds ({speed_mbps:.2f} MB/s)")
    print(f"Initial connections: {initial_connections}, Final connections: {chunk_manager.current_connections}")
    
    if DC_PERFORMANCE_PROFILE and dc_performance:
        print("\nDatacenter Performance:")
        for dc_id, stats in dc_performance.items():
            if stats["success"] > 0:
                print(f"  DC{dc_id}: {stats['success']} successful chunks, {stats['failure']} failures, "
                      f"{stats['avg_speed']:.2f} MB/s average speed")


async def download_last_saved_message():
    async with TelegramClient('session_name', api_id, api_hash) as client:
        # Set client parameters for better performance
        client.flood_sleep_threshold = 120  # Higher threshold for flood wait
        
        await client.connect()
        
        if not await client.is_user_authorized():
            print("You need to authenticate first. Please run the auth script.")
            return
            
        try:
            saved_messages = await client.get_messages("me", limit=1)
            
            if not saved_messages:
                print("No messages found in Saved Messages.")
                return
                
            last_message = saved_messages[0]
            if not (last_message.media and last_message.document):
                print("The last message does not contain a document or media.")
                return
                
            # Get filename from document attributes
            file_name = None
            for attr in last_message.document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    file_name = attr.file_name
                    break
            
            if not file_name:
                file_name = f"file_{last_message.id}"
                
            output_file = os.path.join(DOWNLOADS_FOLDER, file_name)
            file_size = last_message.document.size
            
            # Create an empty file with the required size
            with open(output_file, "wb") as f:
                f.truncate(file_size)
            
            # Download the file concurrently
            print(f"Downloading file: {file_name} (Size: {file_size / 1024 / 1024:.2f} MB)")
            await download_file_concurrently(client, last_message, output_file, file_size)
            print(f"Download complete: {output_file}")
                
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    # Configure event loop policy for maximum performance
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        print("Using uvloop for improved performance")
    except ImportError:
        pass  # Fall back to standard event loop
    
    # Run with higher task limit
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda loop, context: None)  # Suppress some warnings
    loop.run_until_complete(download_last_saved_message())