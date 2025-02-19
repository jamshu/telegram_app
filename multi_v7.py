import os
import asyncio
import io
import time
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
MAX_CONCURRENT_DOWNLOADS = 16  # Maximum number of concurrent chunks
CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunk size (increased from 1MB)
REQUEST_SIZE = 1024 * 1024  # 1MB request size for iter_download
MAX_RETRIES = 5  # Increased from 3
RETRY_DELAY = 1  # Reduced from 3 seconds
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB - files larger than this will use more connections

# Ensure downloads directory exists
os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)


async def download_chunk(client, message, start, end, output_file, progress_bar, semaphore, retries=0):
    async with semaphore:
        try:
            # Calculate the chunk size
            chunk_size = end - start + 1
            
            # Use iter_download to download the specific chunk
            buffer = io.BytesIO()
            downloaded = 0
            
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
            
            # Write the buffer to file at the correct position
            with open(output_file, "r+b") as f:
                f.seek(start)
                f.write(buffer.getvalue()[:chunk_size])  # Only write up to chunk_size
                
        except Exception as e:
            if retries < MAX_RETRIES:
                # Exponential backoff with jitter
                delay = RETRY_DELAY * (2 ** retries) * (0.5 + 0.5 * (asyncio.get_event_loop().time() % 1))
                await asyncio.sleep(delay)
                await download_chunk(client, message, start, end, output_file, progress_bar, semaphore, retries + 1)
            else:
                print(f"Failed to download chunk {start}-{end} after {MAX_RETRIES} retries: {e}")


async def download_file_concurrently(client, message, output_file, file_size):
    # Dynamically adjust number of connections based on file size
    if file_size > LARGE_FILE_THRESHOLD:
        # For large files, use more connections up to MAX_CONCURRENT_DOWNLOADS
        connections = min(MAX_CONCURRENT_DOWNLOADS, max(4, file_size // CHUNK_SIZE))
    else:
        # For smaller files, use fewer connections to avoid overhead
        connections = min(8, max(1, file_size // CHUNK_SIZE))
    
    # Calculate chunk size based on connections
    base_chunk_size = file_size // connections
    
    # Create progress bar with smoother update interval
    progress_bar = tqdm(
        total=file_size, 
        desc="Downloading", 
        unit='B', 
        unit_scale=True, 
        unit_divisor=1024,
        mininterval=0.5  # Update at most every 0.5 seconds
    )
    
    # Create semaphore to limit concurrent connections
    semaphore = asyncio.Semaphore(connections)
    
    # Create and gather download tasks
    tasks = []
    for i in range(connections):
        start = i * base_chunk_size
        
        # Make sure the last chunk gets the remainder
        if i == connections - 1:
            end = file_size - 1
        else:
            end = start + base_chunk_size - 1
        
        task = asyncio.create_task(
            download_chunk(client, message, start, end, output_file, progress_bar, semaphore)
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
    print(f"Download completed in {elapsed:.1f} seconds ({speed_mbps:.2f} MB/s)")


async def download_last_saved_message():
    async with TelegramClient('session_name', api_id, api_hash) as client:
        # Set lower-level connection parameters
        client.flood_sleep_threshold = 60  # Increase threshold for flood wait
        
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
    # Configure event loop for maximum performance
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(lambda loop, context: None)  # Suppress some warnings
    
    # Run the main function
    loop.run_until_complete(download_last_saved_message())